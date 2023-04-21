use crate::page::FreePagePageHeader;
use crate::page::PageType;
use crate::page::Pages;
use crate::page::MAX_PAGE_SIZE_POW2;
use crate::page::MIN_PAGE_SIZE_POW2;
use async_recursion::async_recursion;
use futures::future::join_all;
use itertools::Itertools;
use off64::int::create_u64_be;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::max;
use std::cmp::min;
use std::sync::Arc;
use write_journal::Transaction;
use write_journal::WriteJournal;

const ALLOCSTATE_OFFSETOF_FRONTIER: u64 = 0;
const fn ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(page_size_pow2: u8) -> u64 {
  ALLOCSTATE_OFFSETOF_FRONTIER + 8 * u64::from(page_size_pow2 - MIN_PAGE_SIZE_POW2)
}
const ALLOCSTATE_SIZE: u64 = ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(MAX_PAGE_SIZE_POW2 + 1);

pub struct Allocator {
  state_dev_offset: u64,
  pages: Arc<Pages>,
  journal: Arc<WriteJournal>,
  // To avoid needing to write to the entire device at format time to set up linked list of free lpages, we simply record where the next block would be if there's no free lpage available.
  frontier_dev_offset: u64,
  heap_dev_offset: u64,
  heap_size: u64,
  // One device offset (or zero) for each page size.
  free_list_head: Vec<u64>,
}

impl Allocator {
  pub async fn load_from_device(
    dev: &SeekableAsyncFile,
    state_dev_offset: u64,
    pages: Arc<Pages>,
    journal: Arc<WriteJournal>,
    heap_dev_offset: u64,
    heap_size: u64,
  ) -> Self {
    // Getting the buddy of a page using only XOR requires that the heap starts at an address aligned to the lpage size.
    assert_eq!(heap_dev_offset % (1 << pages.lpage_size_pow2), 0);
    let frontier_dev_offset = dev.read_u64_be_at(ALLOCSTATE_OFFSETOF_FRONTIER).await;
    let page_sizes = pages.lpage_size_pow2 - pages.spage_size_pow2;
    let free_list_head = join_all(
      (0..page_sizes).map(|i| dev.read_u64_be_at(ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(i))),
    )
    .await;
    Self {
      free_list_head,
      frontier_dev_offset,
      heap_dev_offset,
      heap_size,
      journal,
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
      create_u64_be(new_head_page_dev_offset).to_vec(),
    );
    self.free_list_head[pow2_idx] = new_head_page_dev_offset;
  }

  async fn detach_page_from_free_list(
    &mut self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let hdr = self
      .pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await
      .unwrap();
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
  }

  // Returns the dev offset of the page pointed to at the head of the free list for the page size. If it's not zero, it will be detached from the list (updating the head). The page will be marked as used. The new head page's prev pointer will be updated.
  async fn try_consume_page_at_free_list_head(
    &mut self,
    txn: &mut Transaction,
    page_size_pow2: u8,
  ) -> Option<u64> {
    let page_dev_offset = self.get_free_list_head(page_size_pow2);
    if page_dev_offset == 0 {
      return None;
    };
    let new_free_page = self
      .pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await
      .unwrap()
      .next;
    self.update_free_list_head(txn, page_size_pow2, new_free_page);
    self
      .pages
      .update_page_header::<FreePagePageHeader>(txn, new_free_page, |h| h.prev = 0)
      .await;
    Some(page_dev_offset)
  }

  /// Updates the page's prev and next pointers in its header. Updates the free list's head.
  async fn insert_page_into_free_list(
    &mut self,
    txn: &mut Transaction,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let cur_head = self.get_free_list_head(page_size_pow2);
    self.update_free_list_head(txn, page_size_pow2, page_dev_offset);
    // Detect double free or incorrect page size.
    assert!({
      let cur = self
        .pages
        .read_page_header_type_and_size(page_dev_offset)
        .await;
      cur.0 != PageType::FreePage && cur.1 == page_size_pow2
    });
    self
      .pages
      .write_page_header(txn, page_dev_offset, FreePagePageHeader {
        prev: 0,
        next: cur_head,
      })
      .await;
  }

  async fn allocate_new_block_and_then_allocate_lpage(&mut self, txn: &mut Transaction) -> u64 {
    let lpage_size = 1 << self.pages.lpage_size_pow2;
    let block_dev_offset = self.frontier_dev_offset;
    let new_frontier = block_dev_offset + self.pages.block_size;
    if new_frontier > self.heap_dev_offset + self.heap_size {
      panic!("out of space");
    };
    // We don't need to use overlay as we have our own copy in `self.frontier_dev_offset`.
    txn.write(
      self.state_dev_offset + ALLOCSTATE_OFFSETOF_FRONTIER,
      create_u64_be(new_frontier).to_vec(),
    );
    self.frontier_dev_offset = new_frontier;
    // This code is subtle:
    // - The first lpage of a block is always reserved for the metadata lpage, so we should not mark that as a free page.
    // - The first data lpage (second lpage) will be immediately returned for usage, so we should not mark that as a free page.
    // - The first free data lpage (third lpage) needs to be inserted into the free list.
    // - The last data lpage (last lpage) needs to have a `next` of zero.
    // - All other lpages are also free, but can simply link to their phsyical previous and next lpages instead of repeatedly inserting into the free list. This way, only the first free data lpage needs to be inserted into the free list.
    for (left, lpage_dev_offset, right) in (block_dev_offset..=new_frontier)
      .step_by(usz!(lpage_size))
      .skip(1)
      .tuple_windows()
    {
      self
        .pages
        .write_page_header(txn, lpage_dev_offset, FreePagePageHeader {
          prev: if left == block_dev_offset { 0 } else { left },
          next: if right == new_frontier { 0 } else { right },
        })
        .await;
    }
    self.insert_page_into_free_list(
      txn,
      block_dev_offset + 2 * lpage_size,
      self.pages.lpage_size_pow2,
    );
    block_dev_offset + lpage_size
  }

  /// This function will write the FreePagePageHeader to all split pages that aren't returned for the requested allocation, and VoidPageHeader to the returned allocated page, so all page headers will remain in a consistent state.
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
        // There is no lpage to break, so create new block at frontier.
        self.allocate_new_block_and_then_allocate_lpage(txn).await
      }
      None => {
        // Find or create a larger page.
        let larger_page_dev_offset = self.allocate_page(txn, page_size_pow2 + 1).await;
        // Split the larger page in two, and release right page (we'll take the left one).
        let right_page_dev_offset = larger_page_dev_offset + (1 << page_size_pow2);
        self.insert_page_into_free_list(txn, right_page_dev_offset, page_size_pow2);
        larger_page_dev_offset
      }
    };
    // If we don't initialise it, its page header won't have a size, and `write_page_header` won't work.
    self
      .pages
      .initialise_new_page(txn, page_dev_offset, page_size_pow2)
      .await;
    page_dev_offset
  }

  pub async fn allocate(&mut self, txn: &mut Transaction, size: u64) -> u64 {
    let pow2 = max(
      self.pages.spage_size_pow2,
      size.next_power_of_two().ilog2().try_into().unwrap(),
    );
    assert!(pow2 <= self.pages.lpage_size_pow2);
    self.allocate_page(txn, pow2).await
  }

  #[async_recursion]
  pub async fn release(&mut self, txn: &mut Transaction, page_dev_offset: u64) {
    let (typ, page_size_pow2) = self
      .pages
      .read_page_header_type_and_size(page_dev_offset)
      .await;
    // Our current system design/logic only allows releasing deleted inodes by reaper, or from merging released buddies (this function), so let's be strict.
    assert!(
      typ == PageType::DeletedInode || typ == PageType::ObjectSegmentData || typ == PageType::Void
    );
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.pages.lpage_size_pow2 {
      let buddy_page_dev_offset = page_dev_offset ^ (1 << page_size_pow2);
      if self
        .pages
        .read_page_header_type_and_size(buddy_page_dev_offset)
        .await
        == (PageType::FreePage, page_size_pow2)
      {
        // Buddy is also free. We must clear both page headers before merging them to avoid stale reads of their headers.
        // For the left page, since we'll recursively call `release` on it, it needs to have correct size in its header for the recursive call (which will read the size) to work correctly.
        let (left_page, right_page) = (
          min(page_dev_offset, buddy_page_dev_offset),
          max(page_dev_offset, buddy_page_dev_offset),
        );
        self
          .pages
          .initialise_new_page(txn, left_page, page_size_pow2 + 1)
          .await;
        self
          .pages
          .initialise_new_page(txn, right_page, page_size_pow2)
          .await;
        // Merge by freeing parent larger page.
        let parent_page_dev_offset = page_dev_offset & !((1 << (page_size_pow2 + 1)) - 1);
        self.release(txn, parent_page_dev_offset).await;
        return;
      };
    };
    // This will overwrite the page's header.
    self.insert_page_into_free_list(txn, page_dev_offset, page_size_pow2);
  }
}
