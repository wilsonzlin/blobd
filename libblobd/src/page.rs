use off64::usz;
use off64::Off64Int;
use write_journal::Transaction;
use write_journal::WriteJournal;

pub(crate) const MIN_PAGE_SIZE_POW2: u8 = 8;
pub(crate) const MAX_PAGE_SIZE_POW2: u8 = 64;

// Bytes to reserve per page.
const PAGE_HEADER_CAP_POW2: u8 = 4;
const PAGE_HEADER_CAP: u64 = 1 << PAGE_HEADER_CAP_POW2;

const FREE_PAGE_OFFSETOF_PREV: u64 = 1;
const FREE_PAGE_OFFSETOF_NEXT: u64 = FREE_PAGE_OFFSETOF_PREV + 5;

const OBJECT_SEGMENT_DATA_TAG: u8 = 255;
const OBJECT_SEGMENT_DATA_OFFSETOF_BUCKET_ID: u64 = 1;
const OBJECT_SEGMENT_DATA_OFFSETOF_OBJECT_ID: u64 = OBJECT_SEGMENT_DATA_OFFSETOF_BUCKET_ID + 5;

const ACTIVE_INODE_TAG: u8 = 254;
const ACTIVE_INODE_OFFSETOF_NEXT: u64 = 1;

const INCOMPLETE_INODE_TAG: u8 = 253;
const INCOMPLETE_INODE_OFFSETOF_PREV: u64 = 1;
const INCOMPLETE_INODE_OFFSETOF_NEXT: u64 = INCOMPLETE_INODE_OFFSETOF_PREV + 5;
const INCOMPLETE_INODE_OFFSETOF_CREATED_HOUR: u64 = INCOMPLETE_INODE_OFFSETOF_NEXT + 5;

const DELETED_INODE_TAG: u8 = 252;
const DELETED_INODE_OFFSETOF_NEXT: u64 = 1;
const DELETED_INODE_OFFSETOF_DELETED_SEC: u64 = DELETED_INODE_OFFSETOF_NEXT + 5;

pub(crate) trait PageHeader {
  /// Many device offsets can be reduced by 1 byte by right shifting by MIN_MIN_PAGE_SIZE_POW2 because it can't refer to anything more granular than a page. Remember to left shift when deserialising.
  fn serialize(&self, out: &mut [u8]);
  /// WARNING: This method must assert that the tag value is correct, as it isn't checked before calling this method.
  fn deserialize(raw: &[u8]) -> Self;
}

pub(crate) struct FreePagePageHeader {
  pub size_pow2: u8,
  pub prev: u64,
  pub next: u64,
}

impl PageHeader for FreePagePageHeader {
  fn serialize(&self, out: &mut [u8]) {
    assert!(self.size_pow2 >= MIN_PAGE_SIZE_POW2 && self.size_pow2 <= MAX_PAGE_SIZE_POW2);
    out[0] = self.size_pow2;
    out.write_u40_be_at(FREE_PAGE_OFFSETOF_PREV, self.prev >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(FREE_PAGE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
  }

  fn deserialize(raw: &[u8]) -> Self {
    let size_pow2 = raw[0];
    assert!(size_pow2 >= MIN_PAGE_SIZE_POW2 && size_pow2 <= MAX_PAGE_SIZE_POW2);
    Self {
      size_pow2,
      prev: raw.read_u40_be_at(FREE_PAGE_OFFSETOF_PREV) << MIN_PAGE_SIZE_POW2,
      next: raw.read_u40_be_at(FREE_PAGE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
    }
  }
}

pub(crate) struct ObjectSegmentDataPageHeader {
  pub bucket_id: u64,
  pub object_id: u64,
}

impl PageHeader for ObjectSegmentDataPageHeader {
  fn serialize(&self, out: &mut [u8]) {
    out[0] = OBJECT_SEGMENT_DATA_TAG;
    out.write_u40_be_at(OBJECT_SEGMENT_DATA_OFFSETOF_BUCKET_ID, self.bucket_id);
    out.write_u64_be_at(OBJECT_SEGMENT_DATA_OFFSETOF_OBJECT_ID, self.object_id);
  }

  fn deserialize(raw: &[u8]) -> Self {
    assert_eq!(raw[0], OBJECT_SEGMENT_DATA_TAG);
    Self {
      bucket_id: raw.read_u40_be_at(OBJECT_SEGMENT_DATA_OFFSETOF_BUCKET_ID),
      object_id: raw.read_u40_be_at(OBJECT_SEGMENT_DATA_OFFSETOF_OBJECT_ID),
    }
  }
}

pub(crate) struct ActiveInodePageHeader {
  // Device offset of next inode in bucket linked list, or zero if tail.
  pub next: u64,
}

impl PageHeader for ActiveInodePageHeader {
  fn serialize(&self, out: &mut [u8]) {
    out[0] = ACTIVE_INODE_TAG;
    out.write_u40_be_at(ACTIVE_INODE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
  }

  fn deserialize(raw: &[u8]) -> Self {
    assert_eq!(raw[0], ACTIVE_INODE_TAG);
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

impl PageHeader for IncompleteInodePageHeader {
  fn serialize(&self, out: &mut [u8]) {
    out[0] = INCOMPLETE_INODE_TAG;
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
    assert_eq!(raw[0], INCOMPLETE_INODE_TAG);
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
  fn serialize(&self, out: &mut [u8]) {
    out[0] = DELETED_INODE_TAG;
    out.write_u40_be_at(DELETED_INODE_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(DELETED_INODE_OFFSETOF_DELETED_SEC, self.deleted_sec);
  }

  fn deserialize(raw: &[u8]) -> Self {
    assert_eq!(raw[0], DELETED_INODE_TAG);
    Self {
      next: raw.read_u40_be_at(DELETED_INODE_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
      deleted_sec: raw.read_u40_be_at(DELETED_INODE_OFFSETOF_DELETED_SEC),
    }
  }
}

pub(crate) struct Pages {
  block_mask: u64,
  journal: WriteJournal, // To access the overlay.
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
  pub fn new(journal: WriteJournal, spage_size_pow2: u8, lpage_size_pow2: u8) -> Self {
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
    Self {
      block_mask,
      block_size,
      journal,
      lpage_size_pow2,
      spage_size_pow2,
    }
  }

  fn get_page_header_dev_offset(&self, page_dev_offset: u64) -> u64 {
    debug_assert_eq!(page_dev_offset & ((1 << self.spage_size_pow2) - 1), 0);
    let metadata_lpage = page_dev_offset & !self.block_mask;
    let offset_within_metadata_lpage =
      (page_dev_offset & self.block_mask) >> (self.spage_size_pow2 - PAGE_HEADER_CAP_POW2);
    // This is faster than `metadata_lpage + offset_within_metadata_lpage`.
    metadata_lpage | offset_within_metadata_lpage
  }

  pub async fn check_if_page_is_free_page_of_size(
    &self,
    page_dev_offset: u64,
    want_size_pow2: u8,
  ) -> bool {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(hdr_dev_offset, PAGE_HEADER_CAP)
      .await;
    raw[0] == want_size_pow2
  }

  pub async fn read_page_header<H: PageHeader>(&self, page_dev_offset: u64) -> H {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(hdr_dev_offset, PAGE_HEADER_CAP)
      .await;
    H::deserialize(&raw)
  }

  pub fn write_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    h: H,
  ) {
    let hdr_dev_offset = self.get_page_header_dev_offset(page_dev_offset);
    let mut out = vec![0u8; usz!(PAGE_HEADER_CAP)];
    let raw = h.serialize(&mut out);
    txn.write_with_overlay(hdr_dev_offset, out);
  }

  pub async fn update_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    f: impl FnOnce(&mut H) -> (),
  ) {
    let mut hdr = self.read_page_header(page_dev_offset).await;
    f(&mut hdr);
    self.write_page_header(txn, page_dev_offset, hdr);
  }
}
