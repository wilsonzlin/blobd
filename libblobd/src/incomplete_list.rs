use crate::allocator::Allocator;
use crate::inode::release_inode;
use crate::page::IncompleteInodePageHeader;
use crate::page::PagesMut;
use chrono::Utc;
use off64::create_u48_be;
use off64::usz;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
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
const SIZE: u64 = OFFSETOF_TAIL + 5;

/// WARNING: All methods on this struct must be called from within a transaction, **including non-mut ones.** If *any* incomplete inode page changes type or merges without this struct knowing about/performing it, including due to very subtle race conditions and/or concurrency, **corruption will occur.**
/// This is because all methods will read a page's header as a IncompleteInodePageHeader, even if it's a different type or the page has been merged/split and doesn't exist at the time.
/// This also means that the background incomplete reaper loop must also lock the entire state and perform everything in a transaction every iteration.
/// WARNING: Writers must not begin or continue writing if within one hour of an incomplete object's reap time. Otherwise, the reaper may reap the object from under them, and the writer will be writing to released pages. If streaming, check in regularly. Stop before one hour to prevent race conditions and clock drift issues. Commits are safe because they acquire the state lock.
pub struct IncompleteList {
  dev: SeekableAsyncFile,
  dev_offset: u64,
  head: u64,
  tail: u64,
  reap_after_hours: u32,
}

fn get_now_hour() -> u32 {
  let hr = Utc::now().timestamp() / 60 / 60;
  hr.try_into().unwrap()
}

impl IncompleteList {
  pub fn load_from_device(dev: SeekableAsyncFile, dev_offset: u64, reap_after_hours: u32) -> Self {
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
      reap_after_hours,
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
      IncompleteInodePageHeader {
        created_hour: get_now_hour(),
        prev: self.tail,
        next: 0,
      },
    );
    if self.head == 0 {
      self.update_head(txn, page_dev_offset);
    };
    if self.tail != 0 {
      pages
        .update_page_header::<IncompleteInodePageHeader>(txn, self.tail, |i| {
          i.next = page_dev_offset
        })
        .await;
    };
    self.update_tail(txn, page_dev_offset);
  }

  /// WARNING: This does not update, overwrite, or clear the page header.
  pub async fn detach(
    &mut self,
    txn: &mut Transaction,
    pages: &mut PagesMut,
    page_dev_offset: u64,
  ) {
    let hdr = pages
      .read_page_header::<IncompleteInodePageHeader>(page_dev_offset)
      .await;
    if hdr.prev == 0 {
      self.update_head(txn, hdr.next);
    } else {
      pages
        .update_page_header::<IncompleteInodePageHeader>(txn, hdr.prev, |i| i.next = hdr.next)
        .await;
    };
    if hdr.next == 0 {
      self.update_tail(txn, hdr.prev);
    } else {
      pages
        .update_page_header::<IncompleteInodePageHeader>(txn, hdr.next, |i| i.prev = hdr.prev)
        .await;
    };
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
      .read_page_header_with_size_and_type::<IncompleteInodePageHeader>(page_dev_offset)
      .await;
    if get_now_hour() - hdr.created_hour < self.reap_after_hours {
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
    self.detach(txn, pages, page_dev_offset).await;
    true
  }
}
