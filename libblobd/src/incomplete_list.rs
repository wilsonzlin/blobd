use crate::deleted_list::DeletedList;
use crate::object::OBJECT_OFF;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
use crate::page::Pages;
use crate::util::get_now_sec;
use chrono::Utc;
use off64::int::create_u48_be;
use off64::int::Off64AsyncReadInt;
use off64::usz;
use seekable_async_file::SeekableAsyncFile;
use std::sync::Arc;
use write_journal::Transaction;

/*

INCOMPLETE LIST
===============

Doubly-linked list of inodes that are incomplete.

Structure
---------

u48 head
u48 tail

*/

const OFFSETOF_HEAD: u64 = 0;
const OFFSETOF_TAIL: u64 = OFFSETOF_HEAD + 5;
pub(crate) const INCOMPLETE_LIST_STATE_SIZE: u64 = OFFSETOF_TAIL + 5;

/// WARNING: All methods on this struct must be called from within a transaction, **including non-mut ones.** If *any* incomplete inode page changes type or merges without this struct knowing about/performing it, including due to very subtle race conditions and/or concurrency, **corruption will occur.**
/// This is because all methods will read a page's header as a IncompleteInodePageHeader, even if it's a different type or the page has been merged/split and doesn't exist at the time.
/// This also means that the background incomplete reaper loop must also lock the entire state and perform everything in a transaction every iteration.
/// WARNING: Writers must not begin or continue writing if within one hour of an incomplete object's reap time. Otherwise, the reaper may reap the object from under them, and the writer will be writing to released pages. If streaming, check in regularly. Stop before one hour to prevent race conditions and clock drift issues. Commits are safe because they acquire the state lock.
pub(crate) struct IncompleteList {
  dev_offset: u64,
  dev: SeekableAsyncFile,
  head: u64,
  pages: Arc<Pages>,
  reap_objects_after_secs: u64,
  tail: u64,
}

impl IncompleteList {
  pub async fn load_from_device(
    dev: SeekableAsyncFile,
    dev_offset: u64,
    pages: Arc<Pages>,
    reap_objects_after_secs: u64,
  ) -> Self {
    let head = dev.read_u48_be_at(dev_offset + OFFSETOF_HEAD).await;
    let tail = dev.read_u48_be_at(dev_offset + OFFSETOF_TAIL).await;
    Self {
      dev_offset,
      dev,
      head,
      pages,
      reap_objects_after_secs,
      tail,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    let raw = vec![0u8; usz!(INCOMPLETE_LIST_STATE_SIZE)];
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
  pub async fn attach(&mut self, txn: &mut Transaction, page_dev_offset: u64, page_size_pow2: u8) {
    self
      .pages
      .write_page_header(txn, page_dev_offset, ObjectPageHeader {
        deleted_sec: None,
        metadata_size_pow2: page_size_pow2,
        next: 0,
        prev: self.tail,
        state: ObjectState::Incomplete,
      });
    if self.head == 0 {
      self.update_head(txn, page_dev_offset);
    };
    if self.tail != 0 {
      self
        .pages
        .update_page_header::<ObjectPageHeader>(txn, self.tail, |i| {
          i.next = page_dev_offset;
        })
        .await;
    };
    self.update_tail(txn, page_dev_offset);
  }

  /// WARNING: This does not update, overwrite, or clear the page header.
  pub async fn detach(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    let hdr = self
      .pages
      .read_page_header::<ObjectPageHeader>(page_dev_offset)
      .await;
    if hdr.prev == 0 {
      self.update_head(txn, hdr.next);
    } else {
      self
        .pages
        .update_page_header::<ObjectPageHeader>(txn, hdr.prev, |i| i.next = hdr.next)
        .await;
    };
    if hdr.next == 0 {
      self.update_tail(txn, hdr.prev);
    } else {
      self
        .pages
        .update_page_header::<ObjectPageHeader>(txn, hdr.next, |i| i.prev = hdr.prev)
        .await;
    };
  }

  pub async fn maybe_reap_next(
    &mut self,
    txn: &mut Transaction,
    deleted_list: &mut DeletedList,
  ) -> bool {
    let page_dev_offset = self.head;
    if page_dev_offset == 0 {
      return false;
    };
    let hdr = self
      .pages
      .read_page_header::<ObjectPageHeader>(page_dev_offset)
      .await;

    // SAFETY: Object must still exist because only DeletedList::maybe_reap_next function reaps and we're holding the entire State lock right now so it cannot be running.
    let created_sec = self.dev.read_u48_be_at(OBJECT_OFF.created_ms()).await / 1000;

    // Our read and write streams check state every 60 seconds, so we must not reap anywhere near that time AFTER `reap_objects_after_secs`.
    if get_now_sec() - created_sec < self.reap_objects_after_secs + 3600 {
      return false;
    };

    // We don't release directly. It's better to have just one reaper, for simplicity and safety.
    // TODO List reasons why.
    // SAFETY:
    // - We are holding the entire State lock right now.
    // - Incomplete objects can be deleted directly, but since we're holding the lock, it's impossible for that to be happening right now.
    // - The page header will be updated using the overlay, so the changes will be seen immediately, and definitely after we've released the lock.
    // - The deletion reaper won't release until well after deletion time.
    // WARNING: Detach first.
    self.detach(txn, page_dev_offset).await;
    deleted_list.attach(txn, page_dev_offset).await;
    true
  }
}
