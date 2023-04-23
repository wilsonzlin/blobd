use crate::util::div_mod_pow2;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::create_u64_be;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use tracing::info;
use std::sync::Arc;
use write_journal::Transaction;
use write_journal::WriteJournal;

pub(crate) const MIN_PAGE_SIZE_POW2: u8 = 8;
pub(crate) const MAX_PAGE_SIZE_POW2: u8 = 32;

// Bytes to reserve per page.
const PAGE_HEADER_CAP_POW2: u8 = 4;
pub(crate) const PAGE_HEADER_CAP: u64 = 1 << PAGE_HEADER_CAP_POW2;

const FREE_PAGE_OFFSETOF_PREV: u64 = 0;
const FREE_PAGE_OFFSETOF_NEXT: u64 = FREE_PAGE_OFFSETOF_PREV + 5;

const OBJECT_OFFSETOF_PREV: u64 = 0;
const OBJECT_OFFSETOF_NEXT: u64 = OBJECT_OFFSETOF_PREV + 5;
const OBJECT_OFFSETOF_DELETED_SEC: u64 = OBJECT_OFFSETOF_NEXT + 5;
const OBJECT_OFFSETOF_META_SIZE_AND_STATE: u64 = OBJECT_OFFSETOF_DELETED_SEC + 5;

pub(crate) trait PageHeader {
  /// Many device offsets can be reduced by 1 byte by right shifting by MIN_PAGE_SIZE_POW2 because it can't refer to anything more granular than a page. Remember to left shift when deserialising.
  fn serialize(&self, out: &mut [u8]);
  fn deserialize(raw: &[u8]) -> Self;
}

pub(crate) struct FreePagePageHeader {
  pub prev: u64,
  pub next: u64,
}

impl PageHeader for FreePagePageHeader {
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

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  // Avoid 0 to detect uninitialised/missing/corrupt state.
  Incomplete = 1,
  Committed,
  Deleted,
}

pub(crate) struct ObjectPageHeader {
  // Device offset of prev/next inode in incomplete/deleted/bucket linked list, or zero if tail.
  pub prev: u64,
  pub next: u64,
  // Timestamp in seconds since epoch.
  pub deleted_sec: Option<u64>,
  pub state: ObjectState,
  pub metadata_size_pow2: u8,
}

impl PageHeader for ObjectPageHeader {
  fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(OBJECT_OFFSETOF_PREV, self.prev >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(OBJECT_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(OBJECT_OFFSETOF_DELETED_SEC, self.deleted_sec.unwrap_or(0));
    out[usz!(OBJECT_OFFSETOF_META_SIZE_AND_STATE)] =
      (self.metadata_size_pow2 << 3) | (self.state as u8);
  }

  fn deserialize(raw: &[u8]) -> Self {
    let b = raw[usz!(OBJECT_OFFSETOF_META_SIZE_AND_STATE)];
    Self {
      prev: raw.read_u40_be_at(OBJECT_OFFSETOF_PREV) << MIN_PAGE_SIZE_POW2,
      next: raw.read_u40_be_at(OBJECT_OFFSETOF_NEXT) << MIN_PAGE_SIZE_POW2,
      deleted_sec: Some(raw.read_u40_be_at(OBJECT_OFFSETOF_DELETED_SEC)).filter(|ts| *ts != 0),
      state: ObjectState::from_u8(b & 0b111).unwrap(),
      metadata_size_pow2: b >> 3,
    }
  }
}

pub(crate) struct Pages {
  block_mask: u64,
  // To access the overlay.
  journal: Arc<WriteJournal>,
  heap_dev_offset: u64,
  block_size_pow2: u8,
  /// WARNING: Do not modify after creation.
  pub block_size: u64,
  /// WARNING: Do not modify after creation.
  pub lpage_size_pow2: u8,
  /// WARNING: Do not modify after creation.
  pub spage_size_pow2: u8,
}

impl Pages {
  pub fn new(
    journal: Arc<WriteJournal>,
    heap_dev_offset: u64,
    spage_size_pow2: u8,
    lpage_size_pow2: u8,
  ) -> Pages {
    assert!(spage_size_pow2 >= MIN_PAGE_SIZE_POW2);
    assert!(lpage_size_pow2 >= spage_size_pow2 && lpage_size_pow2 <= MAX_PAGE_SIZE_POW2);
    // For fast bitwise calculations to be correct, the heap needs to be aligned to `2^lpage_size_pow2` i.e. start at an address that is a multiple of the largest page size in bytes.
    assert_eq!(mod_pow2(heap_dev_offset, lpage_size_pow2), 0);
    // `lpage` means a page of the largest size. `spage` means a page of the smallest size. A data lpage contains actual data, while a metadata lpage contains the free page bitmap for all pages in the following N data lpages (see following code for value of N). Both are lpages (i.e. pages of the largest page size). A data lpage can have X pages, where X is how many pages of all sizes and offsets it could have.
    // A metadata lpage and the following data lpages it covers constitute a block.
    // The page count per lpage is equal to `2 * (2^lpage_size_pow2 / 2^spage_size_pow2)` which is identical to `2^(1 + lpage_size_pow2 - spage_size_pow2)`. It's equivalent to the count of nodes in a full binary tree, where leaf nodes are the spages and the root node is the sole lpage.
    let pages_per_lpage_pow2 = 1 + lpage_size_pow2 - spage_size_pow2;
    // Calculate how many data lpages we can track in one metadata lpage; we need one bit to track one page. This is equal to `2^lpage_size_pow2 * 8 / 2^pages_per_lpage_pow2`, which is identical to `2^(lpage_size_pow2 + 3 - pages_per_lpage_pow2)`.
    let data_lpages_max_pow2 = lpage_size_pow2 + 3 - pages_per_lpage_pow2;
    // To keep calculations fast, we only store for the next N data lpages where N is a power of two minus one. This way, any device offset in those data lpages can get the device offset of the start of their corresponding metadata lpage with a simple bitwise AND, and the size of a block is simply `2^data_lpages_max_pow2 * 2^lpage_size_pow2` since one data lpage is effectively replaced with a metadata lpage. However, this does mean that the metadata lpage wastes some space.
    let block_size_pow2 = data_lpages_max_pow2 + lpage_size_pow2;
    let block_size = 1 << block_size_pow2;
    let block_mask = block_size - 1;
    info!(block_size, "page config");
    Pages {
      block_mask,
      block_size_pow2,
      block_size,
      heap_dev_offset,
      journal,
      lpage_size_pow2,
      spage_size_pow2,
    }
  }

  #[allow(unused)]
  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }

  #[allow(unused)]
  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  fn assert_valid_page_dev_offset(&self, page_dev_offset: u64) {
    // Must be in the heap.
    assert!(
      page_dev_offset >= self.heap_dev_offset,
      "page dev offset {page_dev_offset} is not in the heap"
    );
    // Must not be in a metadata lpage. Note that the heap is only aligned to lpage, not an entire block, so we must first subtract the heap dev offset.
    assert!(
      mod_pow2(page_dev_offset - self.heap_dev_offset, self.block_size_pow2) >= self.lpage_size(),
      "page dev offset {page_dev_offset} is in a metadata lpage"
    );
    // Must be a multiple of the spage size.
    assert_eq!(
      mod_pow2(page_dev_offset, self.spage_size_pow2),
      0,
      "page dev offset {page_dev_offset} is not a multiple of spage"
    );
  }

  fn get_page_free_bit_offset(&self, page_dev_offset: u64, page_size_pow2: u8) -> (u64, u64) {
    self.assert_valid_page_dev_offset(page_dev_offset);
    let metadata_lpage_dev_offset = page_dev_offset & !self.block_mask;
    // We store our bitmap like a binary tree, where the root node is the lpage, and leaf nodes are spages.
    // Let's assume each bit is like an array element with a u64 index, and the array starts at index 1. Then, given `spage_pow2 == 2` and `lpage_pow2 == 16`:
    // Index 1  (0b0001) = page16_0 => page_pow2 == lpage_pow2 - (63 - lead_zeros) = 16 - (63 - 63) = 16
    // Index 2  (0b0010) = page15_0 => page_pow2 == lpage_pow2 - (63 - lead_zeros) = 16 - (63 - 62) = 15
    // Index 3  (0b0011) = page15_1 => page_pow2 == lpage_pow2 - (63 - lead_zeros) = 16 - (63 - 62) = 15
    // Index 4  (0b0100) = page14_0 => page_pow2 == lpage_pow2 - (63 - lead_zeros) = 16 - (63 - 61) = 14
    // Index 5  (0b0101) = page14_1 => ...
    // Index 6  (0b0110) = page14_2
    // Index 7  (0b0111) = page14_3
    // Index 8  (0b1000) = page13_0
    // Index 9  (0b1001) = page13_1
    // Index 10 (0b1010) = page13_2
    // ...
    // Based on the above pattern, we can determine that the index for a page of size 2^S and zero-based position N,
    // the 1-based index I is `(1 << (lpage_pow2 - S)) + N`, where `N = (page_dev_offset - block_offset) / 2^S`.
    // Now:
    // - We split our bitmaps into 64-bit integers to reduce load on overlay bucket.
    // - Indices are 0-based, not 1-based.
    // So:
    // - Get the u64 value at element `(I - 1) / 64`, which is at dev offset `metadata_lpage_dev_offset + ((I - 1) / 64) * 8`.
    // - Get the bit at position `(I - 1) % 64`.
    let n = div_pow2(page_dev_offset - metadata_lpage_dev_offset, page_size_pow2);
    let i = (1 << (self.lpage_size_pow2 - page_size_pow2)) + n;
    let (elem, bit) = div_mod_pow2(i - 1, 6);
    let elem_dev_offset = metadata_lpage_dev_offset + elem * 8;
    (elem_dev_offset, bit)
  }

  pub async fn is_page_free(&self, page_dev_offset: u64, page_size_pow2: u8) -> bool {
    let (elem_dev_offset, bit) = self.get_page_free_bit_offset(page_dev_offset, page_size_pow2);
    let bitmap = self
      .journal
      .read_with_overlay(elem_dev_offset, 8)
      .await
      .read_u64_be_at(0);
    (bitmap & (1 << bit)) != 0
  }

  pub async fn mark_page_as_free(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let (elem_dev_offset, bit) = self.get_page_free_bit_offset(page_dev_offset, page_size_pow2);
    let bitmap = self
      .journal
      .read_with_overlay(elem_dev_offset, 8)
      .await
      .read_u64_be_at(0);
    txn.write_with_overlay(elem_dev_offset, create_u64_be(bitmap | (1 << bit)).to_vec());
  }

  pub async fn mark_page_as_not_free(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let (elem_dev_offset, bit) = self.get_page_free_bit_offset(page_dev_offset, page_size_pow2);
    let bitmap = self
      .journal
      .read_with_overlay(elem_dev_offset, 8)
      .await
      .read_u64_be_at(0);
    txn.write_with_overlay(
      elem_dev_offset,
      create_u64_be(bitmap & !(1 << bit)).to_vec(),
    );
  }

  pub async fn read_page_header<H: PageHeader>(&self, page_dev_offset: u64) -> H {
    self.assert_valid_page_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(page_dev_offset, PAGE_HEADER_CAP)
      .await;
    H::deserialize(&raw)
  }

  pub fn write_page_header<H: PageHeader>(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    h: H,
  ) {
    self.assert_valid_page_dev_offset(page_dev_offset);
    let mut out = vec![0u8; usz!(PAGE_HEADER_CAP)];
    h.serialize(&mut out);
    txn.write_with_overlay(page_dev_offset, out);
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
