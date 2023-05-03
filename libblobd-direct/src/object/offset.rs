use off64::u64;

/*

Structure
---------

u8 page_size_pow2
u8 state
u64 id
u40 size
u48 created_sec
u16 key_len
u8[] key
u48[] lpage_page_dev_offset
u48[] tail_page_dev_offset
// Put whatever you want here, it'll have the same lifecycle as the object. This could be raw bytes, serialised map, packed array of structs, slotted data, whatever.
u16 assoc_data_len
u8[] assoc_data

*/

#[derive(Clone, Copy)]
pub(crate) struct ObjectMetadataOffsets {
  pub key_len: u16,
  pub lpage_count: u32,
  pub tail_page_count: u8,
  pub assoc_data_len: u16,
}

impl ObjectMetadataOffsets {
  pub fn page_size_pow2(self) -> u64 {
    0
  }

  pub fn state(self) -> u64 {
    self.page_size_pow2() + 1
  }

  pub fn id(self) -> u64 {
    self.state() + 1
  }

  pub fn size(self) -> u64 {
    self.id() + 8
  }

  pub fn created_sec(self) -> u64 {
    self.size() + 5
  }

  pub fn key_len(self) -> u64 {
    self.created_sec() + 6
  }

  pub fn key(self) -> u64 {
    self.key_len() + 2
  }

  pub fn lpages(self) -> u64 {
    self.key() + u64!(self.key_len)
  }

  pub fn lpage(self, idx: u32) -> u64 {
    self.lpages() + 6 * u64!(idx)
  }

  pub fn tail_pages(self) -> u64 {
    self.lpage(self.lpage_count)
  }

  pub fn tail_page(self, idx: u8) -> u64 {
    self.tail_pages() + 6 * u64!(idx)
  }

  pub fn assoc_data_len(self) -> u64 {
    self.tail_page(self.tail_page_count)
  }

  pub fn assoc_data(self) -> u64 {
    self.assoc_data_len() + 2
  }

  pub fn _total_size(self) -> u64 {
    self.assoc_data() + u64!(self.assoc_data_len)
  }
}
