use crate::allocator::Allocator;
use crate::inode::release_inode;
use crate::page::DeletedInodePageHeader;
use crate::page::Pages;
use chrono::Utc;
use off64::int::create_u48_be;
use off64::int::Off64AsyncReadInt;
use off64::u64;
use off64::usz;
use seekable_async_file::SeekableAsyncFile;
use std::sync::Arc;
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

/// WARNING: Same safety requirements and warnings as `IncompleteList`.
/// WARNING: Readers and writers must check if inode still exists while reading/writing no later than `DELETE_REAP_DELAY_SEC_MIN / 2` seconds since the last check. Otherwise, the reaper may reap the object from under them, and the reader/writer will be reading from/writing to released pages. Be conservative to prevent race conditions and clock drift issues.
pub struct DeletedList {
  dev_offset: u64,
  dev: SeekableAsyncFile,
  head: u64,
  pages: Arc<Pages>,
  tail: u64,
}

impl DeletedList {
  pub async fn load_from_device(
    dev: SeekableAsyncFile,
    dev_offset: u64,
    pages: Arc<Pages>,
  ) -> Self {
    let head = dev.read_u48_be_at(dev_offset + OFFSETOF_HEAD).await;
    let tail = dev.read_u48_be_at(dev_offset + OFFSETOF_TAIL).await;
    Self {
      dev,
      dev_offset,
      head,
      pages,
      tail,
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
  pub async fn attach(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    self
      .pages
      .write_page_header(txn, page_dev_offset, DeletedInodePageHeader {
        deleted_sec: Utc::now().timestamp().try_into().unwrap(),
        next: 0,
      });
    if self.head == 0 {
      self.update_head(txn, page_dev_offset);
    };
    if self.tail != 0 {
      self
        .pages
        .update_page_header::<DeletedInodePageHeader>(txn, self.tail, |i| i.next = page_dev_offset)
        .await;
    };
    self.update_tail(txn, page_dev_offset);
  }

  pub async fn maybe_reap_next(&mut self, txn: &mut Transaction, alloc: &mut Allocator) -> bool {
    let page_dev_offset = self.head;
    if page_dev_offset == 0 {
      return false;
    };
    let (page_size_pow2, hdr) = self
      .pages
      .read_page_header_and_size::<DeletedInodePageHeader>(page_dev_offset)
      .await
      .unwrap();
    // Our read and write streams check state every 60 seconds, so we must not reap anywhere near that time.
    if u64!(Utc::now().timestamp()) - hdr.deleted_sec < 3600 {
      return false;
    };
    // `alloc.release` will clear page header.
    release_inode(
      txn,
      &self.dev,
      &self.pages,
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
