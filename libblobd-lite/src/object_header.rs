use crate::journal::IJournal;
use crate::page::MIN_PAGE_SIZE_POW2;
use crate::util::mod_pow2;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use std::sync::Arc;
use write_journal::Transaction;

const OBJECT_HEADER_SIZE_POW2: u8 = 4;
pub(crate) const OBJECT_HEADER_SIZE: u64 = 1 << OBJECT_HEADER_SIZE_POW2;

// These device offsets can be reduced by 1 byte by right shifting by MIN_PAGE_SIZE_POW2 because it can't refer to anything more granular than a page. Remember to left shift when deserialising.
const OBJECT_OFFSETOF_PREV: u64 = 0;
const OBJECT_OFFSETOF_NEXT: u64 = OBJECT_OFFSETOF_PREV + 5;
const OBJECT_OFFSETOF_DELETED_SEC: u64 = OBJECT_OFFSETOF_NEXT + 5;
const OBJECT_OFFSETOF_META_SIZE_AND_STATE: u64 = OBJECT_OFFSETOF_DELETED_SEC + 5;

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  // Avoid 0 to detect uninitialised/missing/corrupt state.
  Incomplete = 1,
  Committed,
  Deleted,
}

pub(crate) struct ObjectHeader {
  // Device offset of prev/next inode in incomplete/deleted/bucket linked list, or zero if tail.
  pub prev: u64,
  pub next: u64,
  // Timestamp in seconds since epoch.
  pub deleted_sec: Option<u64>,
  pub state: ObjectState,
  pub metadata_size_pow2: u8,
}

impl ObjectHeader {
  pub fn serialize(&self, out: &mut [u8]) {
    out.write_u40_be_at(OBJECT_OFFSETOF_PREV, self.prev >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(OBJECT_OFFSETOF_NEXT, self.next >> MIN_PAGE_SIZE_POW2);
    out.write_u40_be_at(OBJECT_OFFSETOF_DELETED_SEC, self.deleted_sec.unwrap_or(0));
    out[usz!(OBJECT_OFFSETOF_META_SIZE_AND_STATE)] =
      (self.metadata_size_pow2 << 3) | (self.state as u8);
  }

  pub fn deserialize(raw: &[u8]) -> Self {
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

#[derive(Clone)]
pub(crate) struct Headers {
  journal: Arc<dyn IJournal>,
  heap_dev_offset: u64,
  spage_size_pow2: u8,
}

impl Headers {
  pub fn new(journal: Arc<dyn IJournal>, heap_dev_offset: u64, spage_size_pow2: u8) -> Self {
    Self {
      journal,
      heap_dev_offset,
      spage_size_pow2,
    }
  }

  fn assert_valid_page_dev_offset(&self, page_dev_offset: u64) {
    // Must be in the heap.
    assert!(
      page_dev_offset >= self.heap_dev_offset,
      "page dev offset {page_dev_offset} is not in the heap"
    );
    // Must be a multiple of the spage size.
    assert_eq!(
      mod_pow2(page_dev_offset, self.spage_size_pow2),
      0,
      "page dev offset {page_dev_offset} is not a multiple of spage"
    );
  }

  pub async fn read_header(&self, page_dev_offset: u64) -> ObjectHeader {
    self.assert_valid_page_dev_offset(page_dev_offset);
    let raw = self
      .journal
      .read_with_overlay(page_dev_offset, OBJECT_HEADER_SIZE)
      .await;
    ObjectHeader::deserialize(&raw)
  }

  pub fn write_header(&self, txn: &mut Transaction, page_dev_offset: u64, h: ObjectHeader) {
    self.assert_valid_page_dev_offset(page_dev_offset);
    let mut out = vec![0u8; usz!(OBJECT_HEADER_SIZE)];
    h.serialize(&mut out);
    txn.write_with_overlay(page_dev_offset, out);
  }

  pub async fn update_header(
    &self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    f: impl FnOnce(&mut ObjectHeader) -> (),
  ) {
    let mut hdr = self.read_header(page_dev_offset).await;
    f(&mut hdr);
    self.write_header(txn, page_dev_offset, hdr);
  }
}
