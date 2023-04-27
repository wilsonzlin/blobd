/**

Structure
---------

We cannot store head data inline as we need to know the object metadata size in order to allocate a page for it, but we cannot know any inline head data length unless we know the object metadata size.

u16 key_len
u8[] key
u48[] lpage_page_dev_offset
u48[] tail_page_dev_offset
// Put whatever you want here, it'll have the same lifecycle as the object. This could be raw bytes, serialised map, packed array of structs, slotted data, whatever.
u16 assoc_data_len
u8[] assoc_data

**/

// This makes it so that a read of the object fields up to and including the key is at most exactly 512 bytes, which is a well-aligned well-sized no-waste read from most SSDs.
pub const OBJECT_KEY_LEN_MAX: u16 = 510;

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

  pub fn key_len(self) -> u64 {
    0
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
