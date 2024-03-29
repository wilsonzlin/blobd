use crate::allocator::Allocator;
use crate::backing_store::uring::URING_LEN_MAX;
use crate::backing_store::BackingStore;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::ByteConsumer;
use bufpool::buf::Buf;
use off64::int::create_u24_le;
use off64::int::create_u32_le;
use off64::int::create_u40_be;
use off64::int::Off64ReadInt;
use off64::u32;
use off64::u8;
use off64::usz;
use parking_lot::Mutex;
use std::cmp::min;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tracing::info;

/*

Structure

{
  u8 == 255 key_is_hash // Reserve 0 for end-of-bundle indicator (it's strictly illegal to have an empty key).
  u8[32] key_hash
} | {
  u8 > 0 key_len
  u8[key_len] key
} key
{
  u1 is_inline
  {
    u7 == 127 is_inline_and_longer_than_127_bytes
    u32 inline_len
    u8[inline_len] inline_data
  } | {
    u7 inline_len_less_than_127_bytes
    u8[inline_len] inline_data
  }
} | {
  u40be dev_offset_rshift9 // Must be big endian so that first bit is flag for `is_inline`.
  u24 size_minus1 // Subtract one so that the largest value can fit (zero will be handled by inline mode).
} data

*/

pub(crate) const LPAGE_SIZE_POW2: u8 = 24; // Must be 24 because size field is u24.
pub(crate) const SPAGE_SIZE_POW2_MIN: u8 = 9; // Must be 9 because of RSHIFT9.
pub(crate) const OBJECT_SIZE_MAX: usize = 1 << LPAGE_SIZE_POW2;

// This should be as small as possible. Using only a few bytes out of an allocated page is wasteful; overflowing a bundle is fatal (requires an expensive offline migration).
pub(crate) const OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD: usize = 7;

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub(crate) enum ObjectTupleKey {
  Hash([u8; 32]),
  Literal(TinyBuf),
}

impl ObjectTupleKey {
  pub fn from_raw_and_hash(raw: TinyBuf, hash: blake3::Hash) -> Self {
    if raw.len() <= 32 {
      ObjectTupleKey::Literal(raw)
    } else {
      ObjectTupleKey::Hash(hash.into())
    }
  }

  pub fn from_raw(raw: TinyBuf) -> Self {
    if raw.len() <= 32 {
      ObjectTupleKey::Literal(raw)
    } else {
      // Only hash if necessary, save CPU.
      ObjectTupleKey::Hash(blake3::hash(&raw).into())
    }
  }

  pub fn hash(&self) -> [u8; 32] {
    match self {
      ObjectTupleKey::Hash(h) => h.clone(),
      ObjectTupleKey::Literal(l) => blake3::hash(l).into(),
    }
  }

  pub fn serialise<T: Write>(&self, out: &mut T) {
    match self {
      ObjectTupleKey::Hash(h) => {
        out.write_all(&[255]).unwrap();
        out.write_all(h).unwrap();
      }
      ObjectTupleKey::Literal(l) => {
        out.write_all(&[u8!(l.len())]).unwrap();
        out.write_all(l).unwrap();
      }
    };
  }

  pub fn deserialise<T: AsRef<[u8]>>(raw: &mut ByteConsumer<T>) -> Self {
    match raw.consume(1)[0] {
      255 => ObjectTupleKey::Hash(raw.consume(32).try_into().unwrap()),
      n => ObjectTupleKey::Literal(TinyBuf::from_slice(raw.consume(n.into()))),
    }
  }
}

#[derive(Clone, Debug)]
pub(crate) enum ObjectTupleData {
  Inline(TinyBuf),
  // WARNING: Do not reorder fields, the serialised MessagePack format does not store field names.
  Heap { size: u32, dev_offset: u64 },
}

impl ObjectTupleData {
  pub fn len(&self) -> usize {
    match self {
      ObjectTupleData::Inline(d) => d.len(),
      ObjectTupleData::Heap { size, .. } => usz!(*size),
    }
  }

  pub fn serialise<T: Write>(&self, out: &mut T) {
    match self {
      ObjectTupleData::Inline(i) => {
        if i.len() < 127 {
          out.write_all(&[u8!(i.len()) | 0b1000_0000]).unwrap();
        } else {
          out.write_all(&[127 | 0b1000_0000]).unwrap();
          out.write_all(&create_u32_le(u32!(i.len()))).unwrap();
        };
        out.write_all(&i).unwrap();
      }
      ObjectTupleData::Heap { size, dev_offset } => {
        assert!(dev_offset >> 9 < (1 << 39));
        out.write_all(&create_u40_be(dev_offset >> 9)).unwrap();
        out
          .write_all(&create_u24_le(size.checked_sub(1).unwrap()))
          .unwrap();
      }
    };
  }

  pub fn deserialise<T: AsRef<[u8]>>(raw: &mut ByteConsumer<T>) -> Self {
    if raw[0] & 0b1000_0000 != 0 {
      let ilen = raw.consume(1)[0] & 0x7f;
      let len = if ilen < 127 {
        usz!(ilen)
      } else {
        usz!(raw.consume(4).read_u32_le_at(0))
      };
      ObjectTupleData::Inline(TinyBuf::from_slice(raw.consume(len)))
    } else {
      let dev_offset = raw.consume(5).read_u40_be_at(0) << 9;
      let size = raw.consume(3).read_u24_le_at(0) + 1;
      ObjectTupleData::Heap { size, dev_offset }
    }
  }
}

pub(crate) fn get_bundle_index_for_key(hash: &[u8; 32], bundle_count: u64) -> u64 {
  // Read as big endian so we always use trailing bytes.
  hash.read_u64_be_at(24) % bundle_count
}

// This exists to provide an iterator, possibly saving the cost of a pointless Vec allocation if it's not needed by the caller.
pub(crate) struct BundleDeserialiser<T: AsRef<[u8]>>(ByteConsumer<T>);

impl<T: AsRef<[u8]>> BundleDeserialiser<T> {
  pub fn new(raw: T) -> Self {
    Self(ByteConsumer::new(raw))
  }
}

impl<T: AsRef<[u8]>> Iterator for BundleDeserialiser<T> {
  type Item = (ObjectTupleKey, ObjectTupleData);

  fn next(&mut self) -> Option<Self::Item> {
    if self.0.is_empty() || self.0[0] == 0 {
      return None;
    };
    Some((
      ObjectTupleKey::deserialise(&mut self.0),
      ObjectTupleData::deserialise(&mut self.0),
    ))
  }
}

pub(crate) fn serialise_bundle(
  pages: &Pages,
  tuples: impl IntoIterator<Item = (ObjectTupleKey, ObjectTupleData)>,
) -> Buf {
  let mut buf = pages.allocate(pages.spage_size());
  for (key, data) in tuples {
    key.serialise(&mut buf);
    data.serialise(&mut buf);
  }
  if buf.len() < usz!(pages.spage_size()) {
    // End of tuples marker.
    buf.push(0);
  };
  // The buffer must be aligned for O_DIRECT.
  // SAFETY: We allocated it with this size.
  unsafe {
    buf.set_len(usz!(pages.spage_size()));
  }
  buf
}

pub(crate) async fn format_device_for_tuples(
  dev: &Arc<dyn BackingStore>,
  pages: &Pages,
  heap_dev_offset: u64,
) {
  let bufsize = ceil_pow2(URING_LEN_MAX, pages.spage_size_pow2);
  let mut blank = pages.slow_allocate_with_zeros(bufsize);
  for offset in (0..heap_dev_offset).step_by(usz!(bufsize)) {
    let size = min(heap_dev_offset - offset, bufsize);
    if size == bufsize {
      blank = dev.write_at(offset, blank).await;
    } else {
      dev
        .write_at(offset, pages.slow_allocate_with_zeros(size))
        .await;
    }
  }
}

pub(crate) async fn load_tuples_from_device(
  dev: &Arc<dyn BackingStore>,
  pages: &Pages,
  metrics: &BlobdMetrics,
  heap_allocator: Arc<Mutex<Allocator>>,
  heap_dev_offset: u64,
) {
  let bufsize = ceil_pow2(URING_LEN_MAX, pages.spage_size_pow2);
  let mut object_count = 0;
  for offset in (0..heap_dev_offset).step_by(usz!(bufsize)) {
    let progress = format!("{:.2}%", offset as f64 / heap_dev_offset as f64 * 100.0);
    info!(progress, object_count, "loading tuples");
    let size = min(heap_dev_offset - offset, bufsize);
    let raw = dev.read_at(offset, size).await;
    for bundle_raw in raw.chunks_exact(usz!(pages.spage_size())) {
      for (_key, data) in BundleDeserialiser::new(bundle_raw) {
        match data {
          ObjectTupleData::Inline(_) => {}
          ObjectTupleData::Heap { size, dev_offset } => {
            heap_allocator.lock().mark_as_allocated(dev_offset, size);
            metrics
              .0
              .heap_object_data_bytes
              .fetch_add(size.into(), Ordering::Relaxed);
          }
        };
        object_count += 1;
      }
    }
  }
  metrics
    .0
    .object_count
    .store(object_count, Ordering::Relaxed);
  info!(object_count, "loaded tuples");
}
