use crate::allocator::Allocations;
use crate::device::IDevice;
use crate::object_header::OBJECT_HEADER_SIZE;
use crate::page::Pages;
use crate::util::ceil_pow2;
use crate::util::div_mod_pow2;
use off64::int::Off64ReadInt;
use off64::u8;
use std::sync::Arc;

/**

Structure
---------

We cannot store head data inline as we need to know the inode size in order to allocate a fragment for it, but we cannot know any inline head data length unless we know the inode size. For simplicity and flexibility, we just use a fragment for tail data, instead of storing it inline which would make it subject to the free space within the same fragmented tile.

The ordering of these fields is important (and somewhat strange/seemingly random), as we want to avoid multiple small reads.
- **write_object** requires `size`, `obj_id`, `key_len`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **find_inode_in_bucket** requires `obj_id`, `key_len`, `key`, and `next_inode_dev_offset_or_zero_if_end`.
- **read_object** requires *find_inode_in_bucket* as well as `size`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **commit_object** requires `obj_id`.

u8[16] _reserved_by_header
u48 created_ms
u40 size
u64 obj_id
u16 key_len
u8[] key
u48[] lpage_page_dev_offset
u48[] tail_page_dev_offset
// Put whatever you want here, it'll have the same lifecycle as the object. This could be raw bytes, serialised map, packed array of structs, slotted data, whatever.
u16 assoc_data_len
u8[] assoc_data

**/

#[derive(Clone, Copy)]
pub(crate) struct ObjectOffsets {
  key_len: u16,
  lpage_count: u64,
  tail_page_count: u8,
  assoc_data_len: u16,
}

impl ObjectOffsets {
  pub fn with_key_len(self, key_len: u16) -> Self {
    Self { key_len, ..self }
  }

  pub fn with_lpages(self, lpage_count: u64) -> Self {
    Self {
      lpage_count,
      ..self
    }
  }

  pub fn with_tail_pages(self, tail_page_count: u8) -> Self {
    Self {
      tail_page_count,
      ..self
    }
  }

  pub fn with_assoc_data_len(self, assoc_data_len: u16) -> Self {
    Self {
      assoc_data_len,
      ..self
    }
  }

  pub fn _reserved_by_header(self) -> u64 {
    0
  }

  pub fn created_ms(self) -> u64 {
    self._reserved_by_header() + OBJECT_HEADER_SIZE
  }

  pub fn size(self) -> u64 {
    self.created_ms() + 6
  }

  pub fn id(self) -> u64 {
    self.size() + 5
  }

  pub fn key_len(self) -> u64 {
    self.id() + 8
  }

  pub fn key(self) -> u64 {
    self.key_len() + 2
  }

  pub fn lpages(self) -> u64 {
    self.key() + u64::from(self.key_len)
  }

  pub fn lpage(self, idx: u64) -> u64 {
    self.lpages() + 6 * idx
  }

  pub fn tail_pages(self) -> u64 {
    self.lpage(self.lpage_count)
  }

  pub fn tail_page(self, idx: u8) -> u64 {
    self.tail_pages() + 6 * u64::from(idx)
  }

  pub fn assoc_data_len(self) -> u64 {
    self.tail_page(self.tail_page_count)
  }

  pub fn assoc_data(self) -> u64 {
    self.assoc_data_len() + 2
  }

  pub fn _total_size(self) -> u64 {
    self.assoc_data() + u64::from(self.assoc_data_len)
  }
}

/// WARNING: This is only safe to use for getting offset of fields up to and including `key`. Call `with_*` methods to get offsets of other fields.
pub(crate) const OBJECT_OFF: ObjectOffsets = ObjectOffsets {
  assoc_data_len: 0,
  key_len: 0,
  lpage_count: 0,
  tail_page_count: 0,
};

// This makes it so that a read of the object fields up to and including the key is at most exactly 512 bytes, which is a well-aligned well-sized no-waste read from most SSDs. In case you're worried that it's not long enough, this is 497 bytes:
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
// aaaaaaaaaaaaaaaaa
pub const OBJECT_KEY_LEN_MAX: u16 = 497;

// Two benefits of this over Vec<u8>:
// - Length type is u8, which is the type for `tail_page_count`.
// - No heap allocations; small enough to copy.
// How it works:
// - Page sizes can only be from `2^8` to `2^32` (see `{MIN,MAX}_PAGE_SIZE_POW2`).
// - There are only `32 - 8 = 24` page sizes.
// - It takes 5 bits to represent 24 distinct values.
// - There can be up to 24 entries, each taking 5 bits. It will take 5 bits to represent the length.
// - We can't fit 125 bits neatly, so we split up into two shards (64-bit ints), and use 4 bits in each int to represent the length of up to 12 entries covered by its int, using exactly `4 + 12 * 5 = 64` bits.
#[derive(Clone, Copy)]
pub struct TailPageSizes(u64, u64);

impl TailPageSizes {
  pub fn new() -> Self {
    Self(0, 0)
  }

  fn shard_get_len(shard: u64) -> u8 {
    u8!((shard & (0b1111 << 60)) >> 60)
  }

  fn shard_set_len(shard: &mut u64, len: u8) {
    assert!(len <= 12);
    *shard &= !(0b1111 << 60);
    *shard |= u64::from(len) << 60;
  }

  fn shard_get(shard: u64, idx: u8) -> u8 {
    assert!(idx < 12);
    let shift = idx * 5;
    u8!((shard & (0b11111 << shift)) >> shift)
  }

  fn shard_set(shard: &mut u64, idx: u8, val: u8) {
    assert!(val < (1 << 5));
    let shift = idx * 5;
    *shard &= !(0b11111 << shift);
    *shard |= u64::from(val) << shift;
  }

  pub fn len(&self) -> u8 {
    Self::shard_get_len(self.0) + Self::shard_get_len(self.1)
  }

  pub fn get(&self, idx: u8) -> Option<u8> {
    if idx >= self.len() {
      return None;
    };
    Some(if idx < 12 {
      Self::shard_get(self.0, idx)
    } else {
      Self::shard_get(self.1, idx - 12)
    })
  }

  pub fn push(&mut self, pow2: u8) {
    let idx = self.len();
    if idx < 12 {
      Self::shard_set(&mut self.0, idx, pow2);
      Self::shard_set_len(&mut self.0, idx + 1);
    } else {
      Self::shard_set(&mut self.1, idx - 12, pow2);
      Self::shard_set_len(&mut self.1, idx - 12 + 1);
    };
  }
}

impl IntoIterator for TailPageSizes {
  type IntoIter = TailPageSizesIter;
  type Item = (u8, u8);

  fn into_iter(self) -> Self::IntoIter {
    TailPageSizesIter(self, 0)
  }
}

// Convenient iterator that provides `u8` indices instead of `.enumerate()` and `u8!(idx)`.
pub struct TailPageSizesIter(TailPageSizes, u8);

impl Iterator for TailPageSizesIter {
  type Item = (u8, u8);

  fn next(&mut self) -> Option<Self::Item> {
    let idx = self.1;
    let entry = self.0.get(idx)?;
    self.1 += 1;
    Some((idx, entry))
  }
}

pub(crate) struct ObjectLayout {
  pub lpage_count: u64,
  pub tail_page_sizes_pow2: TailPageSizes,
}

pub(crate) fn calc_object_layout(pages: Pages, object_size: u64) -> ObjectLayout {
  let (lpage_count, tail_size) = div_mod_pow2(object_size, pages.lpage_size_pow2);
  let mut rem = ceil_pow2(tail_size, pages.spage_size_pow2);
  let mut tail_page_sizes_pow2 = TailPageSizes::new();
  loop {
    let pos = rem.leading_zeros();
    if pos == 64 {
      break;
    };
    let pow2 = u8!(63 - pos);
    tail_page_sizes_pow2.push(pow2);
    rem &= !(1 << pow2);
  }
  ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  }
}

#[must_use]
pub(crate) struct ReleasedObject {
  pub object_metadata_size: u64,
  pub object_data_size: u64,
}

/// WARNING: This does not verify the page type, nor detach the inode from whatever list it's on, but will clear the page header via `alloc.release`.
pub(crate) async fn release_object(
  to_free: &mut Allocations,
  dev: &Arc<dyn IDevice>,
  pages: Pages,
  page_dev_offset: u64,
  page_size_pow2: u8,
) -> ReleasedObject {
  let raw = dev.read_at(page_dev_offset, 1 << page_size_pow2).await;
  let object_size = raw.read_u40_be_at(OBJECT_OFF.size());
  let key_len = raw.read_u16_be_at(OBJECT_OFF.key_len());
  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(pages, object_size);

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len());
  for i in 0..lpage_count {
    let page_dev_offset = raw.read_u48_be_at(off.lpage(i));
    to_free.add(page_dev_offset, pages.lpage_size_pow2);
  }
  for (i, tail_page_size_pow2) in tail_page_sizes_pow2 {
    let page_dev_offset = raw.read_u48_be_at(off.tail_page(i));
    to_free.add(page_dev_offset, tail_page_size_pow2);
  }
  to_free.add(page_dev_offset, page_size_pow2);
  ReleasedObject {
    object_data_size: object_size,
    object_metadata_size: off._total_size(),
  }
}
