use crate::ctx::Ctx;
use crate::ctx::State;
use crate::metrics::BlobdMetrics;
use crate::object::release_object;
use crate::object::OBJECT_OFF;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
use crate::page::Pages;
#[cfg(test)]
use crate::test_util::device::TestSeekableAsyncFile as SeekableAsyncFile;
#[cfg(test)]
use crate::test_util::journal::TestTransaction as Transaction;
use crate::util::get_now_sec;
use off64::int::create_u48_be;
use off64::int::Off64AsyncReadInt;
use off64::usz;
#[cfg(not(test))]
use seekable_async_file::SeekableAsyncFile;
use std::cmp::max;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
#[cfg(not(test))]
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
pub(crate) const DELETED_LIST_STATE_SIZE: u64 = OFFSETOF_TAIL + 5;

/// WARNING: Same safety requirements and warnings as `IncompleteList`.
/// WARNING: Readers and writers must check if inode still exists while reading/writing no later than `DELETE_REAP_DELAY_SEC_MIN / 2` seconds since the last check. Otherwise, the reaper may reap the object from under them, and the reader/writer will be reading from/writing to released pages. Be conservative to prevent race conditions and clock drift issues.
pub(crate) struct DeletedList {
  dev_offset: u64,
  dev: SeekableAsyncFile,
  head: u64,
  metrics: Arc<BlobdMetrics>,
  pages: Arc<Pages>,
  reap_objects_after_secs: u64,
  tail: u64,
}

impl DeletedList {
  pub async fn load_from_device(
    dev: SeekableAsyncFile,
    dev_offset: u64,
    pages: Arc<Pages>,
    metrics: Arc<BlobdMetrics>,
    reap_objects_after_secs: u64,
  ) -> Self {
    let head = dev.read_u48_be_at(dev_offset + OFFSETOF_HEAD).await;
    let tail = dev.read_u48_be_at(dev_offset + OFFSETOF_TAIL).await;
    Self {
      dev_offset,
      dev,
      head,
      metrics,
      pages,
      reap_objects_after_secs,
      tail,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    let raw = vec![0u8; usz!(DELETED_LIST_STATE_SIZE)];
    dev.write_at(dev_offset, raw).await;
  }

  fn update_head(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    txn.write(
      self.dev_offset + OFFSETOF_HEAD,
      create_u48_be(page_dev_offset),
    );
    self.head = page_dev_offset;
  }

  fn update_tail(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    txn.write(
      self.dev_offset + OFFSETOF_TAIL,
      create_u48_be(page_dev_offset),
    );
    self.tail = page_dev_offset;
  }

  /// WARNING: This will overwrite the page header.
  pub async fn attach(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    self.metrics.incr_deleted_object_count(txn, 1);
    self
      .pages
      .update_page_header::<ObjectPageHeader>(txn, page_dev_offset, |o| {
        debug_assert_ne!(o.state, ObjectState::Deleted);
        debug_assert_eq!(o.deleted_sec, None);
        o.state = ObjectState::Deleted;
        o.deleted_sec = Some(get_now_sec());
        o.next = 0;
      })
      .await;
    if self.head == 0 {
      self.update_head(txn, page_dev_offset);
    };
    if self.tail != 0 {
      self
        .pages
        .update_page_header::<ObjectPageHeader>(txn, self.tail, |i| {
          debug_assert_eq!(i.state, ObjectState::Deleted);
          debug_assert_ne!(i.deleted_sec, None);
          i.next = page_dev_offset;
        })
        .await;
    };
    self.update_tail(txn, page_dev_offset);
  }
}

/// If there's nothing to reap, `Err(sleep_time_sec)` will be returned.
/// TODO This is currently a free function to work around the fact that Rust won't let us call a mut method on `state.deleted_list` with `&mut state.allocator` even though they are different fields.
pub(crate) async fn maybe_reap_next_deleted(
  state: &mut State,
  metrics: &BlobdMetrics,
  txn: &mut Transaction,
) -> Result<(), u64> {
  let page_dev_offset = state.deleted_list.head;
  if page_dev_offset == 0 {
    return Err(3600);
  };
  let hdr = state
    .deleted_list
    .pages
    .read_page_header::<ObjectPageHeader>(page_dev_offset)
    .await;

  // SAFETY: Object must still exist because only this function reaps and we haven't processed this object yet.
  let created_sec = state
    .deleted_list
    .dev
    .read_u48_be_at(page_dev_offset + OBJECT_OFF.created_ms())
    .await
    / 1000;

  // Our read and write streams check state every 60 seconds, so we must not reap anywhere near that time.
  // Our token must always be able to read the object state, so we must not reap until well after token expires.
  let now = get_now_sec();
  let reap_time = max(
    hdr.deleted_sec.unwrap() + 3600,
    created_sec + state.deleted_list.reap_objects_after_secs + 3600,
  );
  if now < reap_time {
    return Err(reap_time - now);
  };
  // `alloc.release` called by `release_object` will clear the page header.
  let obj = release_object(
    txn,
    &state.deleted_list.dev,
    &state.deleted_list.pages,
    &mut state.allocator,
    page_dev_offset,
    hdr.metadata_size_pow2,
  )
  .await;
  metrics.decr_deleted_object_count(txn, 1);
  metrics.decr_object_count(txn, 1);
  metrics.decr_object_data_bytes(txn, obj.object_data_size);
  metrics.decr_object_metadata_bytes(txn, obj.object_metadata_size);
  state.deleted_list.update_head(txn, hdr.next);
  if hdr.next == 0 {
    state.deleted_list.update_tail(txn, 0);
  };
  Ok(())
}

pub(crate) async fn start_deleted_list_reaper_background_loop(ctx: Arc<Ctx>) {
  loop {
    let (txn, sleep_sec) = {
      let mut state = ctx.state.lock().await;
      let mut txn = ctx.journal.begin_transaction();
      let sleep_sec = maybe_reap_next_deleted(&mut state, &ctx.metrics, &mut txn)
        .await
        .err()
        .unwrap_or(0);
      (txn, sleep_sec)
    };
    ctx.journal.commit_transaction(txn).await;
    sleep(Duration::from_secs(sleep_sec)).await;
  }
}
