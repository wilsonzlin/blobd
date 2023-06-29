use crate::allocator::Allocator;
use crate::backing_store::BackingStore;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use bufpool::buf::Buf;
use off64::int::create_u24_le;
use off64::int::create_u40_be;
use off64::int::Off64ReadInt;
use off64::u8;
use off64::usz;
use std::cmp::min;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;

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
  u7 inline_len
  u8[inline_len] inline_data
} | {
  u40be dev_offset_rshift9 // Must be big endian so that first bit is flag for `is_inline`.
  u24 size_minus1 // Subtract one so that the largest value can fit (zero will be handled by inline mode).
} data

*/

pub(crate) const LPAGE_SIZE_POW2: u8 = 24; // Must be 24 because size field is u24.
pub(crate) const SPAGE_SIZE_POW2_MIN: u8 = 9; // Must be 9 beceausue of RSHIFT9.
pub(crate) const OBJECT_SIZE_MAX: u32 = 1 << LPAGE_SIZE_POW2;

pub(crate) const OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD: usize = 15; // Don't set this too high as otherwise bundle capacities become uneven.

#[derive(PartialEq, Eq, Clone, Hash)]
pub(crate) enum ObjectTupleKey {
  Hash([u8; 32]),
  Literal(TinyBuf),
}

impl ObjectTupleKey {
  pub fn from_raw(raw: TinyBuf, hash: blake3::Hash) -> Self {
    if raw.len() <= 32 {
      ObjectTupleKey::Literal(raw)
    } else {
      ObjectTupleKey::Hash(hash.into())
    }
  }
}

#[derive(Clone)]
pub(crate) enum ObjectTupleData {
  Inline(TinyBuf),
  Heap { size: u32, dev_offset: u64 },
}

pub(crate) struct ObjectTuple {
  pub key: ObjectTupleKey,
  pub data: ObjectTupleData,
}

impl ObjectTuple {
  pub fn serialise(&self, out: &mut Buf) {
    match &self.key {
      ObjectTupleKey::Hash(h) => {
        out.push(255);
        out.extend_from_slice(h);
      }
      ObjectTupleKey::Literal(l) => {
        out.push(u8!(l.len()));
        out.extend_from_slice(l);
      }
    };
    match &self.data {
      ObjectTupleData::Inline(i) => {
        assert!(i.len() < 128);
        out.push(u8!(i.len()) | 0b1000_0000);
        out.extend_from_slice(i);
      }
      ObjectTupleData::Heap { size, dev_offset } => {
        assert!(dev_offset >> 9 < (1 << 39));
        out.extend_from_slice(&create_u40_be(dev_offset >> 9));
        out.extend_from_slice(&create_u24_le(size.checked_sub(1).unwrap()));
      }
    };
  }

  pub fn deserialise(mut raw: &[u8]) -> (Self, &[u8]) {
    macro_rules! consume {
      ($n:expr) => {{
        let (l, r) = raw.split_at($n);
        raw = r;
        l
      }};
    }
    let key = match consume!(1)[0] {
      255 => ObjectTupleKey::Hash(consume!(32).try_into().unwrap()),
      n => ObjectTupleKey::Literal(TinyBuf::from_slice(consume!(n.into()))),
    };
    let data = if raw[0] & 0b1000_0000 != 0 {
      let len = consume!(1)[0] & 0x7f;
      ObjectTupleData::Inline(TinyBuf::from_slice(consume!(len.into())))
    } else {
      let dev_offset = consume!(5).read_u40_be_at(0) << 9;
      let size = consume!(3).read_u24_le_at(0) + 1;
      ObjectTupleData::Heap { size, dev_offset }
    };
    (Self { key, data }, raw)
  }
}

pub(crate) fn get_bundle_index_for_key(hash: &blake3::Hash, bundle_count: u64) -> u64 {
  // Read as big endian so we always use trailing bytes.
  hash.as_bytes().read_u64_be_at(24) % bundle_count
}

pub(crate) async fn load_bundle_from_device(
  dev: &Arc<dyn BackingStore>,
  pages: &Pages,
  bundle_idx: u64,
) -> Vec<ObjectTuple> {
  let bundle_raw = dev
    .read_at(bundle_idx * pages.spage_size(), pages.spage_size())
    .await;
  deserialise_bundle(&bundle_raw)
}

pub(crate) fn deserialise_bundle(mut raw: &[u8]) -> Vec<ObjectTuple> {
  let mut tuples = Vec::new();
  while !raw.is_empty() && raw[0] != 0 {
    let (t, rem) = ObjectTuple::deserialise(raw);
    tuples.push(t);
    raw = rem;
  }
  tuples
}

pub(crate) fn serialise_bundle(
  pages: &Pages,
  tuples: impl IntoIterator<Item = ObjectTuple>,
) -> Buf {
  let mut buf = pages.allocate(pages.spage_size());
  for t in tuples {
    t.serialise(&mut buf);
  }
  if buf.len() < usz!(pages.spage_size()) {
    // End of tuples marker.
    buf.push(0);
  };
  buf
}

pub(crate) async fn format_device_for_tuples(
  dev: &Arc<dyn BackingStore>,
  pages: &Pages,
  heap_dev_offset: u64,
) {
  const BUFSIZE: u64 = 1024 * 1024 * 1024;
  for offset in (0..heap_dev_offset).step_by(usz!(BUFSIZE)) {
    let size = min(heap_dev_offset - offset, BUFSIZE);
    dev
      .write_at(offset, pages.slow_allocate_with_zeros(size))
      .await;
  }
}

pub(crate) struct LoadedTuplesFromDevice {
  pub heap_allocator: Allocator,
}

pub(crate) async fn load_tuples_from_device(
  dev: &Arc<dyn BackingStore>,
  pages: &Pages,
  metrics: &BlobdMetrics,
  heap_dev_offset: u64,
  heap_size: u64,
) -> LoadedTuplesFromDevice {
  let mut heap_allocator =
    Allocator::new(heap_dev_offset, heap_size, pages.clone(), metrics.clone());
  const BUFSIZE: u64 = 1024 * 1024 * 1024;
  for offset in (0..heap_dev_offset).step_by(usz!(BUFSIZE)) {
    let size = min(heap_dev_offset - offset, BUFSIZE);
    let raw = dev.read_at(offset, size).await;
    for bundle_raw in raw.chunks_exact(usz!(pages.spage_size())) {
      for tuple in deserialise_bundle(bundle_raw) {
        match tuple.data {
          ObjectTupleData::Inline(_) => {}
          ObjectTupleData::Heap { size, dev_offset } => {
            heap_allocator.mark_as_allocated(dev_offset, size);
            metrics
              .0
              .heap_object_data_bytes
              .fetch_add(size.into(), Ordering::Relaxed);
          }
        };
        metrics.0.object_count.fetch_add(1, Ordering::Relaxed);
      }
    }
  }
  LoadedTuplesFromDevice { heap_allocator }
}
