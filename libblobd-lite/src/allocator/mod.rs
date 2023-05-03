#[cfg(test)]
pub mod tests;

use crate::metrics::BlobdMetrics;
use crate::page::FreePagePageHeader;
use crate::page::Pages;
use crate::page::MAX_PAGE_SIZE_POW2;
use crate::page::MIN_PAGE_SIZE_POW2;
#[cfg(test)]
use crate::test_util::device::TestSeekableAsyncFile as SeekableAsyncFile;
#[cfg(test)]
use crate::test_util::journal::TestTransaction as Transaction;
use crate::util::floor_pow2;
use crate::util::mod_pow2;
use async_recursion::async_recursion;
use futures::future::join_all;
use off64::int::create_u64_be;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
#[cfg(not(test))]
use seekable_async_file::SeekableAsyncFile;
use std::cmp::max;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::debug;
use tracing::info;
use tracing::trace;
#[cfg(not(test))]
use write_journal::Transaction;

const ALLOCSTATE_OFFSETOF_FRONTIER: u64 = 0;
const fn ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(page_size_pow2: u8) -> u64 {
  ALLOCSTATE_OFFSETOF_FRONTIER + 8 + 8 * ((page_size_pow2 - MIN_PAGE_SIZE_POW2) as u64)
}
pub(crate) const ALLOCSTATE_SIZE: u64 =
  ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(MAX_PAGE_SIZE_POW2 + 1);

pub(crate) struct Allocator {
  state_dev_offset: u64,
  pages: Arc<Pages>,
  metrics: Arc<BlobdMetrics>,
  // To avoid needing to write to the entire device at format time to set up linked list of free lpages, we simply record where the next block would be if there's no free lpage available.
  frontier_dev_offset: u64,
  // This could change during online resizing.
  device_size: Arc<AtomicU64>,
  // One device offset (or zero) for each page size.
  free_list_head: Vec<u64>,
}

impl Allocator {
  pub async fn load_from_device(
    dev: &SeekableAsyncFile,
    device_size: Arc<AtomicU64>,
    state_dev_offset: u64,
    pages: Arc<Pages>,
    metrics: Arc<BlobdMetrics>,
    heap_dev_offset: u64,
  ) -> Self {
    // Getting the buddy of a page using only XOR requires that the heap starts at an address aligned to the lpage size.
    assert_eq!(mod_pow2(heap_dev_offset, pages.lpage_size_pow2), 0);
    let frontier_dev_offset = dev
      .read_u64_be_at(state_dev_offset + ALLOCSTATE_OFFSETOF_FRONTIER)
      .await;
    let free_list_head = join_all((pages.spage_size_pow2..=pages.lpage_size_pow2).map(|i| {
      dev.read_u64_be_at(state_dev_offset + ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(i))
    }))
    .await;
    debug!(frontier_dev_offset, "allocator loaded");
    Self {
      device_size,
      free_list_head,
      frontier_dev_offset,
      metrics,
      pages,
      state_dev_offset,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, state_dev_offset: u64, heap_dev_offset: u64) {
    let mut raw = vec![0u8; usz!(ALLOCSTATE_SIZE)];
    raw.write_u64_be_at(ALLOCSTATE_OFFSETOF_FRONTIER, heap_dev_offset);
    dev.write_at(state_dev_offset, raw).await;
  }

  fn get_free_list_head(&mut self, page_size_pow2: u8) -> u64 {
    let pow2_idx = usz!(page_size_pow2 - self.pages.spage_size_pow2);
    self.free_list_head[pow2_idx]
  }

  /// - Updates allocator state in memory: ✓
  /// - Updates allocator state on device: ✓
  /// - Updates bitmap: **NO**
  /// - Updates header of sibling page: **NO**
  /// - Updates header of target page: **NO**
  /// - Updates metrics: **NO**
  ///
  /// Updates page pointed to by list head. Doesn't touch the page or anything else.
  fn update_free_list_head(
    &mut self,
    txn: &mut Transaction,
    page_size_pow2: u8,
    new_head_page_dev_offset: u64,
  ) {
    let pow2_idx = usz!(page_size_pow2 - self.pages.spage_size_pow2);
    // We don't need to use overlay as we have our own copy in `self.free_list_head`.
    txn.write(
      self.state_dev_offset + ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(page_size_pow2),
      create_u64_be(new_head_page_dev_offset),
    );
    self.free_list_head[pow2_idx] = new_head_page_dev_offset;
    trace!(
      new_head_page_dev_offset,
      page_size_pow2,
      "updated free list head"
    );
  }

  /// - Updates allocator state in memory: ✓
  /// - Updates allocator state on device: ✓
  /// - Updates bitmap: **NO**
  /// - Updates header of sibling page: ✓
  /// - Updates header of target page: **NO**
  /// - Updates metrics: **NO**
  ///
  /// Updates page pointed to by list head or sibling, whichever is adjacent. Doesn't touch the page or anything else.
  async fn detach_page_from_free_list(
    &mut self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let hdr = self
      .pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await;
    if hdr.prev == 0 {
      // Update head.
      self.update_free_list_head(txn, page_size_pow2, hdr.next);
    } else {
      // Update prev page's next.
      self
        .pages
        .update_page_header::<FreePagePageHeader>(txn, hdr.prev, |h| h.next = hdr.next)
        .await;
    };
    if hdr.next != 0 {
      // Update next page's prev.
      self
        .pages
        .update_page_header::<FreePagePageHeader>(txn, hdr.next, |h| h.prev = hdr.prev)
        .await;
    };
    trace!(
      page_dev_offset,
      page_size_pow2,
      "detached page from free list"
    );
  }

  /// - Updates allocator state in memory: ✓ (via `update_free_list_head`)
  /// - Updates allocator state on device: ✓ (via `update_free_list_head`)
  /// - Updates bitmap: **NO**
  /// - Updates header of sibling page: ✓
  /// - Updates header of target page: **NO**
  /// - Updates metrics: **NO**
  ///
  /// Detaches page at head from list and marks it as used.
  async fn try_consume_page_at_free_list_head(
    &mut self,
    txn: &mut Transaction,
    page_size_pow2: u8,
  ) -> Option<u64> {
    let page_dev_offset = self.get_free_list_head(page_size_pow2);
    if page_dev_offset == 0 {
      return None;
    };
    trace!(page_size_pow2, page_dev_offset, "found free page");
    // We don't need to mark as not free as `allocate_page` will do that.
    let new_free_page = self
      .pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await
      .next;
    self.update_free_list_head(txn, page_size_pow2, new_free_page);
    if new_free_page != 0 {
      self
        .pages
        .update_page_header::<FreePagePageHeader>(txn, new_free_page, |h| h.prev = 0)
        .await;
    };
    Some(page_dev_offset)
  }

  /// - Updates allocator state in memory: ✓ (via `update_free_list_head`)
  /// - Updates allocator state on device: ✓ (via `update_free_list_head`)
  /// - Updates bitmap: **NO**
  /// - Updates header of sibling page: ✓
  /// - Updates header of target page: ✓
  /// - Updates metrics: **NO**
  ///
  /// Attaches page to list, overwriting any existing header. **Does not mark as free.**
  async fn insert_page_into_free_list(
    &mut self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let cur_head = self.get_free_list_head(page_size_pow2);
    self
      .pages
      .write_page_header(txn, page_dev_offset, FreePagePageHeader {
        prev: 0,
        next: cur_head,
      });
    if cur_head != 0 {
      self
        .pages
        .update_page_header::<FreePagePageHeader>(txn, cur_head, |f| f.prev = page_dev_offset)
        .await;
    };
    self.update_free_list_head(txn, page_size_pow2, page_dev_offset);
    trace!(
      page_size_pow2,
      page_dev_offset,
      cur_head,
      "inserted page into free list"
    );
  }

  async fn allocate_new_block_and_then_allocate_lpage(&mut self, txn: &mut Transaction) -> u64 {
    let lpage_size = 1 << self.pages.lpage_size_pow2;
    let block_dev_offset = self.frontier_dev_offset;
    let new_frontier = block_dev_offset + self.pages.block_size;
    info!(block_dev_offset, new_frontier, "allocating new block");
    if new_frontier > self.device_size.load(Ordering::Relaxed) {
      panic!("out of space");
    };
    self.metrics.incr_allocated_block_count(txn, 1);
    // We don't need to use overlay as we have our own copy in `self.frontier_dev_offset`.
    txn.write(
      self.state_dev_offset + ALLOCSTATE_OFFSETOF_FRONTIER,
      create_u64_be(new_frontier),
    );
    self.frontier_dev_offset = new_frontier;

    // Write bitmap of free pages in metadata lpage for block. We must use overlay.
    for i in (0..self.pages.lpage_size()).step_by(8) {
      txn.write_with_overlay(block_dev_offset + i, create_u64_be(u64::MAX));
    }

    // - The first lpage of a block is always reserved for the metadata lpage, so we should not mark that as a free page.
    // - The first data lpage (second lpage) will be immediately returned for usage, so we should not mark that as a free page.
    // - We insert in reverse order so that lpages with a lower offset are used first. This isn't necessary for correctness but may offer some performance benefit due to sequential I/O, and makes testing easier too.
    // It may seem inefficient to insert every lpage in a loop, instead of just building a list and then inserting the head only, but:
    // - We only do this every new block, which should not be very often, so the cost is amortised.
    // - We're only talking about a few thousand elements, so the loop execution is still blisteringly fast (each iteration just reads and writes from a cached mmap page and DashMap). There shouldn't be any noticeable system pause/delay.
    // - We previously used an optimised loop that was subtle and confusing, which lead to some subtle bugs not being discovered. The complexity is not worth the intangible performance gain.
    {
      let mut lpage_dev_offset = new_frontier - lpage_size;
      while lpage_dev_offset >= block_dev_offset + 2 * lpage_size {
        self
          .insert_page_into_free_list(txn, lpage_dev_offset, self.pages.lpage_size_pow2)
          .await;
        lpage_dev_offset -= lpage_size;
      }
    };
    // Mark metadata lpage as used space.
    self.metrics.incr_used_bytes(txn, self.pages.lpage_size());

    // We don't need to mark as not free as `allocate_page` will do that.
    let first_data_lpage_dev_offset = block_dev_offset + lpage_size;
    first_data_lpage_dev_offset
  }

  #[async_recursion]
  async fn allocate_page(&mut self, txn: &mut Transaction, page_size_pow2: u8) -> u64 {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    let page_dev_offset = match self
      .try_consume_page_at_free_list_head(txn, page_size_pow2)
      .await
    {
      Some(page_dev_offset) => page_dev_offset,
      None if page_size_pow2 == self.pages.lpage_size_pow2 => {
        trace!(page_size_pow2, "ran out of lpages, will allocate new block");
        // There is no lpage to break, so create new block at frontier.
        self.allocate_new_block_and_then_allocate_lpage(txn).await
      }
      None => {
        trace!(
          page_size_pow2,
          "ran out of pages, will allocate page of the next bigger size"
        );
        // Find or create a larger page.
        let larger_page_dev_offset = self.allocate_page(txn, page_size_pow2 + 1).await;
        // Split the larger page in two, and release right page (we'll take the left one).
        let right_page_dev_offset = larger_page_dev_offset + (1 << page_size_pow2);
        self
          .insert_page_into_free_list(txn, right_page_dev_offset, page_size_pow2)
          .await;
        self
          .pages
          .mark_page_as_free(txn, right_page_dev_offset, page_size_pow2)
          .await;
        larger_page_dev_offset
      }
    };
    // We must always mark an allocated page as not free, including split pages which definitely cannot be released/merged.
    self
      .pages
      .mark_page_as_not_free(txn, page_dev_offset, page_size_pow2)
      .await;
    trace!(page_size_pow2, page_dev_offset, "allocated page");
    page_dev_offset
  }

  pub async fn allocate_and_ret_with_size(
    &mut self,
    txn: &mut Transaction,
    size: u64,
  ) -> (u64, u8) {
    let pow2 = max(
      self.pages.spage_size_pow2,
      size.next_power_of_two().ilog2().try_into().unwrap(),
    );
    assert!(pow2 <= self.pages.lpage_size_pow2);
    // We increment these metrics here instead of in `Pages::*`, `Allocator::insert_into_free_list`, `Allocator::allocate_page`, etc. as many of those are called during intermediate states, like merging/splitting pages, which aren't actual allocations.
    self.metrics.incr_allocated_page_count(txn, 1);
    self.metrics.incr_used_bytes(txn, 1 << pow2);
    (self.allocate_page(txn, pow2).await, pow2)
  }

  pub async fn allocate(&mut self, txn: &mut Transaction, size: u64) -> u64 {
    self.allocate_and_ret_with_size(txn, size).await.0
  }

  #[async_recursion]
  async fn release_internal(
    &mut self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.pages.lpage_size_pow2 {
      let buddy_page_dev_offset = page_dev_offset ^ (1 << page_size_pow2);
      if self
        .pages
        .is_page_free(buddy_page_dev_offset, page_size_pow2)
        .await
      {
        // Buddy is also free.
        trace!(
          page_dev_offset,
          page_size_pow2,
          buddy_page_dev_offset,
          "buddy is also free"
        );
        self
          .detach_page_from_free_list(txn, buddy_page_dev_offset, page_size_pow2)
          .await;
        self
          .pages
          .mark_page_as_not_free(txn, page_dev_offset, page_size_pow2)
          .await;
        self
          .pages
          .mark_page_as_not_free(txn, buddy_page_dev_offset, page_size_pow2)
          .await;
        // Merge by freeing parent larger page.
        let parent_page_dev_offset = floor_pow2(page_dev_offset, page_size_pow2 + 1);
        self
          .release_internal(txn, parent_page_dev_offset, page_size_pow2 + 1)
          .await;
        return;
      };
    };
    // This will overwrite the page's header.
    self
      .insert_page_into_free_list(txn, page_dev_offset, page_size_pow2)
      .await;
    self
      .pages
      .mark_page_as_free(txn, page_dev_offset, page_size_pow2)
      .await;
  }

  pub async fn release(&mut self, txn: &mut Transaction, page_dev_offset: u64, page_size_pow2: u8) {
    // Similar to `allocate_and_ret_with_size`, we need to change metrics here and use an internal function. See `allocate_and_ret_with_size` for comment explaining why.
    self.metrics.decr_allocated_page_count(txn, 1);
    self.metrics.decr_used_bytes(txn, 1 << page_size_pow2);
    self
      .release_internal(txn, page_dev_offset, page_size_pow2)
      .await
  }
}
