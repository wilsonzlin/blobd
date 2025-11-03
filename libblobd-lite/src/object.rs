use crate::allocator::layout::TailPageSizes;
use crate::allocator::layout::calc_object_layout;
use crate::allocator::pages::Pages;
use crate::overlay::Overlay;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::Off64Read;
use off64::int::Off64ReadInt;
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

u8 state
u8 metadata_size_pow2
u48 next_node_dev_offset
u48 created_ms
u40 size
u128 obj_id
u16 key_len
u8[] key
u48[] lpage_page_dev_offset
u48[] tail_page_dev_offset
// Put whatever you want here, it'll have the same lifecycle as the object. This could be raw bytes, serialised map, packed array of structs, slotted data, whatever.
u16 assoc_data_len
u8[] assoc_data

**/

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  // Avoid 0 to detect uninitialised/missing/corrupt state.
  Incomplete = 1,
  Committed,
}

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

  pub fn state(self) -> u64 {
    0
  }

  pub fn metadata_size_pow2(self) -> u64 {
    self.state() + 1
  }

  pub fn next_node_dev_offset(self) -> u64 {
    self.metadata_size_pow2() + 1
  }

  pub fn created_ms(self) -> u64 {
    self.next_node_dev_offset() + 6
  }

  pub fn size(self) -> u64 {
    self.created_ms() + 6
  }

  pub fn id(self) -> u64 {
    self.size() + 5
  }

  pub fn key_len(self) -> u64 {
    self.id() + 16
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

/// Use this when you need to read fields from an object's metadata, as this will ensure the overlay is consistently used at all times.
/// To use this, first read the raw bytes of the object metadata from disk, then provide to `ctx.obj(raw)`.
/// You do not need to read all fields, if you don't need them all. But if you try to read a field that is not present, you will get a panic.
/// Use this over reading from the raw bytes directly like `raw.read_u40_be_at(OBJECT_OFF.size())`, which was the old way.
/// This also calculates the allocation layout internally, so you can also use this to get the allocation layout or the total metadata size.
#[derive(Clone)]
pub(crate) struct ObjectMeta {
  pub raw: Vec<u8>,
  pub overlay: Arc<Overlay>,
  pub pages: Pages,
}

#[allow(unused)]
impl ObjectMeta {
  pub fn state(&self) -> ObjectState {
    self
      .overlay
      .get_object_state(self.id())
      .unwrap_or_else(|| ObjectState::from_u8(self.raw.read_u8_at(OBJECT_OFF.state())).unwrap())
  }

  pub fn metadata_size_pow2(&self) -> u8 {
    self.raw.read_u8_at(OBJECT_OFF.metadata_size_pow2())
  }

  pub fn metadata_size(&self) -> u64 {
    1 << self.metadata_size_pow2()
  }

  pub fn next_node_dev_offset(&self) -> Option<u64> {
    let raw = self
      .overlay
      .get_object_next(self.id())
      .unwrap_or_else(|| self.raw.read_u48_be_at(OBJECT_OFF.next_node_dev_offset()));
    Some(raw).filter(|&o| o > 0)
  }

  pub fn created_ms(&self) -> u64 {
    self.raw.read_u48_be_at(OBJECT_OFF.created_ms())
  }

  pub fn size(&self) -> u64 {
    self.raw.read_u40_be_at(OBJECT_OFF.size())
  }

  pub fn id(&self) -> u128 {
    self.raw.read_u128_be_at(OBJECT_OFF.id())
  }

  pub fn key_len(&self) -> u16 {
    self.raw.read_u16_be_at(OBJECT_OFF.key_len())
  }

  pub fn key(&self) -> &[u8] {
    self.raw.read_at(OBJECT_OFF.key(), self.key_len().into())
  }

  pub fn lpage_count(&self) -> u64 {
    calc_object_layout(self.pages, self.size()).lpage_count
  }

  pub fn lpage(&self, idx: u64) -> u64 {
    let off = OBJECT_OFF.with_key_len(self.key_len());
    self.raw.read_u48_be_at(off.lpage(idx))
  }

  pub fn tail_page_count(&self) -> u8 {
    calc_object_layout(self.pages, self.size())
      .tail_page_sizes_pow2
      .len()
  }

  pub fn tail_page(&self, idx: u8) -> u64 {
    let off = OBJECT_OFF
      .with_key_len(self.key_len())
      .with_lpages(self.lpage_count());
    self.raw.read_u48_be_at(off.tail_page(idx))
  }

  pub fn tail_page_sizes_pow2(&self) -> TailPageSizes {
    calc_object_layout(self.pages, self.size()).tail_page_sizes_pow2
  }

  pub fn assoc_data_len(&self) -> u16 {
    let off = OBJECT_OFF
      .with_key_len(self.key_len())
      .with_lpages(self.lpage_count())
      .with_tail_pages(self.tail_page_count());
    self.raw.read_u16_be_at(off.assoc_data_len())
  }

  pub fn assoc_data(&self) -> &[u8] {
    let off = OBJECT_OFF
      .with_key_len(self.key_len())
      .with_lpages(self.lpage_count())
      .with_tail_pages(self.tail_page_count());
    self
      .raw
      .read_at(off.assoc_data(), self.assoc_data_len().into())
  }
}
