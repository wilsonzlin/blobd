use crate::ctx::Ctx;
use crate::ctx::State;
use crate::object::OBJECT_OFF;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
use crate::page::Pages;
use crate::util::get_now_sec;
use off64::int::create_u48_be;
use off64::int::Off64AsyncReadInt;
use off64::usz;
use seekable_async_file::SeekableAsyncFile;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
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
          debug_assert_eq!(i.state, ObjectState::Incomplete);
          debug_assert_eq!(i.deleted_sec, None);
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
        .update_page_header::<ObjectPageHeader>(txn, hdr.prev, |i| {
          debug_assert_eq!(i.state, ObjectState::Incomplete);
          debug_assert_eq!(i.deleted_sec, None);
          i.next = hdr.next;
        })
        .await;
    };
    if hdr.next == 0 {
      self.update_tail(txn, hdr.prev);
    } else {
      self
        .pages
        .update_page_header::<ObjectPageHeader>(txn, hdr.next, |i| {
          debug_assert_eq!(i.state, ObjectState::Incomplete);
          debug_assert_eq!(i.deleted_sec, None);
          i.prev = hdr.prev;
        })
        .await;
    };
  }
}

/// If there's nothing to reap, `Err(sleep_time_sec)` will be returned.
/// TODO This is currently a free function to work around the fact that Rust won't let us call a mut method on `state.incomplete_list` with `&mut state.deleted_list` even though they are different fields.
pub(crate) async fn maybe_reap_next_incomplete(
  state: &mut State,
  txn: &mut Transaction,
) -> Result<(), u64> {
  let page_dev_offset = state.incomplete_list.head;
  if page_dev_offset == 0 {
    return Err(3600);
  };

  // SAFETY: Object must still exist because only DeletedList::maybe_reap_next function reaps and we're holding the entire State lock right now so it cannot be running. We even handle the very rare chance that we're about to read an object that has just been created but has not yet been written to mmap; see create_object.
  let created_sec = state
    .incomplete_list
    .dev
    .read_u48_be_at(page_dev_offset + OBJECT_OFF.created_ms())
    .await
    / 1000;

  // Our read and write streams check state every 60 seconds, so we must not reap anywhere near that time AFTER `reap_objects_after_secs`.
  let now = get_now_sec();
  let reap_time = created_sec + state.incomplete_list.reap_objects_after_secs + 3600;
  if now < reap_time {
    return Err(reap_time - now);
  };

  // We don't release directly. It's better to have just one reaper, for simplicity and safety.
  // TODO List reasons why.
  // SAFETY:
  // - We are holding the entire State lock right now.
  // - Incomplete objects can be deleted directly, but since we're holding the lock, it's impossible for that to be happening right now.
  // - Since lock is required to update IncompleteList, if an incomplete object was deleted, it should've been detached and we should not be seeing it right now.
  // - The page header will be updated using the overlay, so the changes will be seen immediately, and definitely after we've released the lock.
  // - The deletion reaper won't release until well after deletion time.
  // WARNING: Detach first.
  state.incomplete_list.detach(txn, page_dev_offset).await;
  state.deleted_list.attach(txn, page_dev_offset).await;
  Ok(())
}

pub(crate) async fn start_incomplete_list_reaper_background_loop(ctx: Arc<Ctx>) {
  loop {
    let (txn, sleep_sec) = {
      let mut state = ctx.state.lock().await;
      let mut txn = ctx.journal.begin_transaction();
      let sleep_sec = maybe_reap_next_incomplete(&mut state, &mut txn)
        .await
        .err()
        .unwrap_or(0);
      (txn, sleep_sec)
    };
    ctx.journal.commit_transaction(txn).await;
    sleep(Duration::from_secs(sleep_sec)).await;
  }
}
