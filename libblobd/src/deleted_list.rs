use crate::allocator::Allocator;
use crate::inode::release_inode;
use crate::page::DeletedInodePageHeader;
use crate::page::PagesMut;
use chrono::Utc;
use off64::create_u48_be;
use off64::usz;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use write_journal::Transaction;

/*

DELETED LIST
============

Singly-linked list of inodes that have been deleted and are pending their visit from the reaper.

Structure
---------

u48 head
u48 tail

*/

const OFFSETOF_HEAD: u64 = 0;
const OFFSETOF_TAIL: u64 = OFFSETOF_HEAD + 5;
const SIZE: u64 = OFFSETOF_TAIL + 5;

pub const DELETE_REAP_DELAY_SEC_MIN: u64 = 3600;

/// WARNING: Same safety requirements and warnings as `IncompleteList`.
/// WARNING: Readers and writers must check if inode still exists while reading/writing no later than `DELETE_REAP_DELAY_SEC_MIN / 2` seconds since the last check. Otherwise, the reaper may reap the object from under them, and the reader/writer will be reading from/writing to released pages. Be conservative to prevent race conditions and clock drift issues.
pub struct DeletedList {
  dev: SeekableAsyncFile,
  dev_offset: u64,
  head: u64,
  tail: u64,
  reap_after_secs: u64,
}

impl DeletedList {
  pub fn load_from_device(dev: SeekableAsyncFile, dev_offset: u64, reap_after_secs: u64) -> Self {
    assert!(reap_after_secs >= DELETE_REAP_DELAY_SEC_MIN);
    let head = dev
      .read_at_sync(dev_offset + OFFSETOF_HEAD, 6)
      .read_u48_be_at(0);
    let tail = dev
      .read_at_sync(dev_offset + OFFSETOF_TAIL, 6)
      .read_u48_be_at(0);
    Self {
      dev,
      dev_offset,
      head,
      tail,
      reap_after_secs,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    let raw = vec![0u8; usz!(SIZE)];
    dev.write_at(dev_offset, raw).await;
  }

  fn update_head(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    txn.write(
      self.dev_offset + OFFSETOF_HEAD,
      create_u48_be(page_dev_offset).to_vec(),
    );
    self.head = page_dev_offset;
  }

  fn update_tail(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    txn.write(
      self.dev_offset + OFFSETOF_TAIL,
      create_u48_be(page_dev_offset).to_vec(),
    );
    self.tail = page_dev_offset;
  }

  /// WARNING: This will overwrite the page header.
  pub async fn attach(
    &mut self,
    txn: &mut Transaction,
    pages: &mut PagesMut,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    pages.write_page_header(
      txn,
      page_dev_offset,
      page_size_pow2,
      DeletedInodePageHeader {
        deleted_sec: Utc::now().timestamp().try_into().unwrap(),
        next: 0,
      },
    );
    if self.head == 0 {
      self.update_head(txn, page_dev_offset);
    };
    if self.tail != 0 {
      pages
        .update_page_header::<DeletedInodePageHeader>(txn, self.tail, |i| i.next = page_dev_offset)
        .await;
    };
    self.update_tail(txn, page_dev_offset);
  }

  pub async fn maybe_reap_next(
    &mut self,
    txn: &mut Transaction,
    pages: &mut PagesMut,
    alloc: &mut Allocator,
  ) -> bool {
    let page_dev_offset = self.head;
    if page_dev_offset == 0 {
      return false;
    };
    let (_, page_size_pow2, hdr) = pages
      .read_page_header_with_size_and_type::<DeletedInodePageHeader>(page_dev_offset)
      .await;
    if u64::try_from(Utc::now().timestamp()).unwrap() - hdr.deleted_sec < self.reap_after_secs {
      return false;
    };
    // `alloc.release` will clear page header.
    release_inode(
      txn,
      &self.dev,
      pages,
      alloc,
      page_dev_offset,
      page_size_pow2,
    )
    .await;
    self.update_head(txn, hdr.next);
    if hdr.next == 0 {
      self.update_tail(txn, 0);
    };
    true
  }
}
