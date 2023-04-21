use chrono::Utc;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u32;
use off64::usz;
use std::sync::Arc;
use write_journal::Transaction;
use write_journal::WriteJournal;

pub(crate) const MIN_PAGE_SIZE_POW2: u8 = 8;
pub(crate) const MAX_PAGE_SIZE_POW2: u8 = 32;

/// WARNING: Do not change the order of entries, as values are significant.
#[derive(Clone, Copy, PartialEq, Eq, FromPrimitive, Debug)]
pub(crate) enum PageType {
  Void, // A void page is one that is not of any other type. All pages must have a valid page header, including type and size, which is why this type exists.
  FreePage,
  ObjectSegmentData,
  ActiveInode,
  IncompleteInode,
  DeletedInode,
}

// Bytes to reserve per page.
const PAGE_HEADER_CAP_POW2: u8 = 4;
const PAGE_HEADER_CAP: u64 = 1 << PAGE_HEADER_CAP_POW2;

const FREE_PAGE_OFFSETOF_PREV: u64 = 0;
const FREE_PAGE_OFFSETOF_NEXT: u64 = FREE_PAGE_OFFSETOF_PREV + 5;

const ACTIVE_INODE_OFFSETOF_NEXT: u64 = 0;

const INCOMPLETE_INODE_OFFSETOF_PREV: u64 = 0;
const INCOMPLETE_INODE_OFFSETOF_NEXT: u64 = INCOMPLETE_INODE_OFFSETOF_PREV + 5;
const INCOMPLETE_INODE_OFFSETOF_CREATED_HOUR: u64 = INCOMPLETE_INODE_OFFSETOF_NEXT + 5;

const DELETED_INODE_OFFSETOF_NEXT: u64 = 0;
const DELETED_INODE_OFFSETOF_DELETED_SEC: u64 = DELETED_INODE_OFFSETOF_NEXT + 5;

pub(crate) trait PageHeader {
  fn typ() -> PageType;
  /// Many device offsets can be reduced by 1 byte by right shifting by MIN_PAGE_SIZE_POW2 because it can't refer to anything more granular than a page. Remember to left shift when deserialising.
  fn serialize(&self, out: &mut [u8]);
  fn deserialize(raw: &[u8]) -> Self;
}

pub(crate) struct VoidPageHeader {}

impl PageHeader for VoidPageHeader {
  fn typ() -> PageType {
    PageType::Void
  }

  fn serialize(&self, _out: &mut [u8]) {}

  fn deserialize(_raw: &[u8]) -> Self {
    Self {}
  }
}

pub(crate) struct FreePagePageHeader {
  pub prev: u64,
  pub next: u64,
}

impl PageHeader for FreePagePageHeader {
  fn typ() -> PageType {
    PageType::FreePage
  }

  fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(FREE_PAGE_OFFSETOF_PREV, self.prev >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(FREE_PAGE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
  }

  fn deserialize(raw: &[u8]) -> Self {
    Self {
      prev: raw.read_u40_be_at(FREE_PAGE_OFFSETOF_PREV) << MIN_PAGE_SIZE_POW2,
      next: raw.read_u40_be_at(FREE_PAGE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
    }
  }
}

pub(crate) struct ObjectSegmentDataPageHeader {}

impl PageHeader for ObjectSegmentDataPageHeader {
  fn typ() -> PageType {
    PageType::ObjectSegmentData
  }

  fn serialize(&self, _out: &mut [u8]) {}

  fn deserialize(_raw: &[u8]) -> Self {
    Self {}
  }
}

pub(crate) struct ActiveInodePageHeader {
  // Device offset of next inode in bucket linked list, or zero if tail.
  pub next: u64,
}

impl PageHeader for ActiveInodePageHeader {
  fn typ() -> PageType {
    PageType::ActiveInode
  }

  fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(ACTIVE_INODE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
  }

  fn deserialize(raw: &[u8]) -> Self {
    Self {
      next: raw.read_u40_be_at(ACTIVE_INODE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
    }
  }
}

pub(crate) struct IncompleteInodePageHeader {
  // Device offset of prev/next inode in incomplete linked list, or zero if tail.
  pub prev: u64,
  pub next: u64,
  // Timestamp in hours since epoch.
  pub created_hour: u32,
}

impl IncompleteInodePageHeader {
  pub fn has_expired(&self, incomplete_objects_expire_after_hours: u32) -> bool {
    let now_hour = u32!(Utc::now().timestamp() / 60 / 60);
    now_hour - self.created_hour >= incomplete_objects_expire_after_hours
  }
}

impl PageHeader for IncompleteInodePageHeader {
  fn typ() -> PageType {
    PageType::IncompleteInode
  }

  fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(
      INCOMPLETE_INODE_OFFSETOF_PREV,
      self.prev >> MIN_PAGE_SIZE_POW2,
    );
    out.write_u40_be_at(
      INCOMPLETE_INODE_OFFSETOF_NEXT,
      self.next >> MIN_PAGE_SIZE_POW2,
    );
    out.write_u32_be_at(INCOMPLETE_INODE_OFFSETOF_CREATED_HOUR, self.created_hour);
  }

  fn deserialize(raw: &[u8]) -> Self {
    Self {
      prev: raw.read_u40_be_at(INCOMPLETE_INODE_OFFSETOF_PREV) << MIN_PAGE_SIZE_POW2,
      next: raw.read_u40_be_at(INCOMPLETE_INODE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
      created_hour: raw.read_u32_be_at(INCOMPLETE_INODE_OFFSETOF_CREATED_HOUR),
    }
  }
}

pub(crate) struct DeletedInodePageHeader {
  // Device offset of next inode in deleted linked list, or zero if tail.
  pub next: u64,
  // Timestamp in seconds since epoch.
  pub deleted_sec: u64,
}

impl PageHeader for DeletedInodePageHeader {
  fn typ() -> PageType {
    PageType::DeletedInode
  }

  fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(DELETED_INODE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(DELETED_INODE_OFFSETOF_DELETED_SEC, self.deleted_sec);
  }

  fn deserialize(raw: &[u8]) -> Self {
    Self {
      next: raw.read_u40_be_at(DELETED_INODE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
      deleted_sec: raw.read_u40_be_at(DELETED_INODE_OFFSETOF_DELETED_SEC),
    }
  }
}

pub(crate) struct Pages {
  block_mask: u64,
  // To access the overlay.
  journal: Arc<WriteJournal>,
  /// WARNING: Do not modify after creation.
  pub block_size: u64,
  /// WARNING: Do not modify after creation.
  pub lpage_size_pow2: u8,
  /// WARNING: Do not modify after creation.
  pub spage_size_pow2: u8,
}

impl Pages {
  /// `spage_size_pow2` must be at least 8 (256 bytes).
  /// `lpage_size_pow2` must be at least `spage_size_pow2` and at most 64.
  /// WARNING: For all these fast bitwise calculations to be correct, the heap needs to be aligned to `2^lpage_size_pow2` i.e. start at an address that is a multiple of the large page size in bytes.
  pub fn new(journal: Arc<WriteJournal>, spage_size_pow2: u8, lpage_size_pow2: u8) -> Pages {
    assert!(spage_size_pow2 >= 8);
    assert!(lpage_size_pow2 >= spage_size_pow2 && lpage_size_pow2 <= 64);
    // `lpage` means a page of the largest size. `spage` means a page of the smallest size. A data lpage contains actual data, while a metadata lpage contains the page headers for all spages in the following N data lpages (see following code for value of N). Both are lpages (i.e. pages of the largest page size). A data lpage can have X spages, where X is how many pages of the smallest size can fit in one page of the largest size.
    // A metadata lpage and the data lpage it covers constitute a block.
    // The spage count per lpage is equal to `2^lpage_size_pow2 / 2^spage_size_pow2` which is identical to `2^(lpage_size_pow2 - spage_size_pow2)`.
    let spage_per_lpage_pow2 = lpage_size_pow2 - spage_size_pow2;
    // In the worst case, an lpage may be broken into spages only, so we need enough capacity to store that many page headers.
    // Calculate how much space we need to store the page headers for all spages in a lpage. This is equal to `2^spage_per_lpage_pow2 * 2^page_header_cap_pow2` which is identical to `2^(spage_per_lpage_pow2 + page_header_cap_pow2)`.
    let data_lpage_metadata_size_pow2 = spage_per_lpage_pow2 + PAGE_HEADER_CAP_POW2;
    // Calculate how many data lpages we can track in one metadata lpage. This is equal to `2^lpage_size_pow2 / 2^data_lpage_metadata_size_pow2`, which is identical to `2^(lpage_size_pow2 - data_lpage_metadata_size_pow2)`.
    let data_lpages_max_pow2 = lpage_size_pow2 - data_lpage_metadata_size_pow2;
    // To keep calculations fast, we only store for the next N data lpages where N is a power of two minus one. This way, any device offset in those data lpages can get the device offset of the start of their corresponding metadata lpage with a simple bitwise AND, and the size of a block is simply `2^data_lpages_max_pow2 * 2^lpage_size_pow2` since one data lpage is effectively replaced with a metadata lpage. However, this does mean that the metadata lpage wastes some space.
    let block_size_pow2 = data_lpages_max_pow2 + lpage_size_pow2;
    let block_size = 1 << block_size_pow2;
    let block_mask = block_size - 1;
    Pages {
      block_mask,
      block_size,
      journal,
      lpage_size_pow2,
      spage_size_pow2,
    }
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }

  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  fn get_page_header_dev_offset(&self, page_dev_offset: u64) -> u64 {
    debug_assert_eq!(page_dev_offset & ((1 << self.spage_size_pow2) - 1), 0);
    let metadata_lpage = page_dev_offset & !self.block_mask;
    let offset_within_metadata_lpage =
      (page_dev_offset & self.block_mask) >> (self.spage_size_pow2 - PAGE_HEADER_CAP_POW2);
    // This is faster than `metadata_lpage + offset_within_metadata_lpage`.
    metadata_lpage | offset_within_metadata_lpage
  }

  fn parse_header_size_and_type(&self, raw: &[u8]) -> (PageType, u8) {
    let typ = PageType::from_u8(raw[0] & 0b111).unwrap();
    let size = raw[0] >> 3;
    // Detect when we have forgotten to initialise a page.
    assert!(size >= self.spage_size_pow2 && size <= self.lpage_size_pow2);
    (typ, size)
  }

  pub async fn read_page_header_type_and_size(&self, page_dev_offset: u64) -> (PageType, u8) {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(hdr_dev_offset, PAGE_HEADER_CAP)
      .await;
    self.parse_header_size_and_type(&raw)
  }

  pub async fn read_page_header_and_size<H: PageHeader>(
    &self,
    page_dev_offset: u64,
  ) -> Option<(u8, H)> {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(hdr_dev_offset, PAGE_HEADER_CAP)
      .await;
    let (typ, size) = self.parse_header_size_and_type(&raw);
    if H::typ() == typ {
      Some((size, H::deserialize(&raw[1..])))
    } else {
      None
    }
  }

  pub async fn read_page_header<H: PageHeader>(&self, page_dev_offset: u64) -> Option<H> {
    self
      .read_page_header_and_size(page_dev_offset)
      .await
      .map(|(_, hdr)| hdr)
  }

  // This is almost the same as `write_page_header`, except it writes the page size directly instead of reading, which is important because in order for `write_page_header` someone has to be the first to set the page size. This is also useful when merging or splitting pages. However, use this carefully, as it will overwrite the size value without any checking. It's dangerous to write the size directly because page headers overlap with pages of other sizes at the same offset.
  pub fn initialise_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
    h: H,
  ) {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let mut out = vec![0u8; usz!(PAGE_HEADER_CAP)];
    out[0] = (page_size_pow2 << 3) | H::typ() as u8;
    h.serialize(&mut out[1..]);
    txn.write_with_overlay(hdr_dev_offset, out);
  }

  pub async fn write_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    h: H,
  ) {
    // To avoid an async read, we could just ask for the page size in this function's parameters, but that adds a lot of extra burden on callers, and we need to perform a read anyway, since any I/O system needs to read the device block into a buffer, apply the write to the buffer, then write the buffer, whether we're using mmap, direct I/O, pwrite, or our own userspace I/O library.
    let (_, page_size_pow2) = self.read_page_header_type_and_size(page_dev_offset).await;
    self.initialise_page_header(txn, page_dev_offset, page_size_pow2, h);
  }

  pub async fn update_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    f: impl FnOnce(&mut H) -> (),
  ) {
    let (_, mut hdr) = self
      .read_page_header_and_size(page_dev_offset)
      .await
      .unwrap();
    f(&mut hdr);
    self.write_page_header(txn, page_dev_offset, hdr).await;
  }
}
