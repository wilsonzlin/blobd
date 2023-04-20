use crate::page::FreePagePageHeader;
use crate::page::Pages;
use crate::page::PagesMut;
use crate::page::MAX_PAGE_SIZE_POW2;
use crate::page::MIN_PAGE_SIZE_POW2;
use async_recursion::async_recursion;
use itertools::Itertools;
use off64::usz;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::max;
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
  journal: Arc<WriteJournal>,
  // To avoid needing to write to the entire device at format time to set up linked list of free lpages, we simply record where the next block would be if there's no free lpage available.
  frontier_dev_offset: u64,
  heap_dev_offset: u64,
  heap_size: u64,
  // One device offset (or zero) for each page size.
  free_list_head: Vec<u64>,
}

impl Allocator {
  pub fn load_from_device(
    dev: &SeekableAsyncFile,
    state_dev_offset: u64,
    pages: &Pages,
    journal: Arc<WriteJournal>,
    heap_dev_offset: u64,
    heap_size: u64,
  ) -> Self {
    // Getting the buddy of a page using only XOR requires that the heap starts at an address aligned to the lpage size.
    assert_eq!(heap_dev_offset % (1 << pages.lpage_size_pow2), 0);
    let frontier_dev_offset = dev.read_u64_be_at(ALLOCSTATE_OFFSETOF_FRONTIER);
    let page_sizes = pages.lpage_size_pow2 - pages.spage_size_pow2;
    let free_list_head = (0..page_sizes)
      .map(|i| dev.read_u64_be_at(ALLOCSTATE_OFFSETOF_PAGE_SIZE_FREE_LIST_HEAD(i)))
      .collect_vec();
    Self {
      state_dev_offset,
      journal,
      frontier_dev_offset,
      heap_dev_offset,
      heap_size,
      free_list_head,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, state_dev_offset: u64, heap_dev_offset: u64) {
    let mut raw = vec![0u8; usz!(ALLOCSTATE_SIZE)];
    raw.write_u64_be_at(ALLOCSTATE_OFFSETOF_FRONTIER, heap_dev_offset);
    dev.write_at(state_dev_offset, raw).await;
  }

  fn get_free_list_head(&mut self, pages: &Pages, page_size_pow2: u8) -> u64 {
    let pow2_idx = usz!(page_size_pow2 - pages.spage_size_pow2);
    self.free_list_head[pow2_idx]
  }

  fn update_free_list_head(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    page_size_pow2: u8,
    new_head_page_dev_offset: u64,
  ) {
    let pow2_idx = usz!(page_size_pow2 - pages.spage_size_pow2);
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
    pages: &Pages,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let hdr = pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await;
    if hdr.prev == 0 {
      // Update head.
      self.update_free_list_head(txn, pages, page_size_pow2, hdr.next);
    } else {
      // Update prev page's next.
      pages
        .update_page_header::<FreePagePageHeader>(txn, hdr.prev, |h| h.next = hdr.next)
        .await;
    };
    if hdr.next != 0 {
      // Update next page's prev.
      pages
        .update_page_header::<FreePagePageHeader>(txn, hdr.next, |h| h.prev = hdr.prev)
        .await;
    };
  }

  // Returns the dev offset of the page pointed to at the head of the free list for the page size. If it's not zero, it will be detached from the list (updating the head). The page will be marked as used. The new head page's prev pointer will be updated.
  async fn try_consume_page_at_free_list_head(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    page_size_pow2: u8,
  ) -> Option<u64> {
    let page_dev_offset = self.get_free_list_head(pages, page_size_pow2);
    if page_dev_offset == 0 {
      return None;
    };
    let new_free_page = pages
      .read_page_header::<FreePagePageHeader>(page_dev_offset)
      .await
      .next;
    self.update_free_list_head(txn, pages, page_size_pow2, new_free_page);
    pages
      .update_page_header::<FreePagePageHeader>(txn, new_free_page, |h| h.prev = 0)
      .await;
    Some(page_dev_offset)
  }

  // Updates the page's prev and next pointers, and marks the page as free. Updates the free list's head.
  fn insert_page_into_free_list(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    let cur_head = self.get_free_list_head(pages, page_size_pow2);
    self.update_free_list_head(txn, pages, page_size_pow2, page_dev_offset);
    pages.write_page_header(txn, page_dev_offset, FreePagePageHeader {
      size_pow2: page_size_pow2,
      prev: 0,
      next: cur_head,
    });
  }

  fn allocate_new_block_and_then_allocate_lpage(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
  ) -> u64 {
    let lpage_size = 1 << pages.lpage_size_pow2;
    let block_dev_offset = self.frontier_dev_offset;
    let new_frontier = block_dev_offset + pages.block_size;
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
      pages.write_page_header(txn, lpage_dev_offset, FreePagePageHeader {
        size_pow2: pages.lpage_size_pow2,
        prev: if left == block_dev_offset { 0 } else { left },
        next: if right == new_frontier { 0 } else { right },
      });
    }
    self.insert_page_into_free_list(
      txn,
      pages,
      block_dev_offset + 2 * lpage_size,
      pages.lpage_size_pow2,
    );
    block_dev_offset + lpage_size
  }

  #[async_recursion]
  async fn allocate_page(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    page_size_pow2: u8,
  ) -> u64 {
    assert!(page_size_pow2 >= pages.spage_size_pow2 && page_size_pow2 <= pages.lpage_size_pow2);
    match self
      .try_consume_page_at_free_list_head(txn, pages, page_size_pow2)
      .await
    {
      Some(page_dev_offset) => page_dev_offset,
      None if page_size_pow2 == pages.lpage_size_pow2 => {
        // There is no lpage to break, so create new block at frontier.
        self.allocate_new_block_and_then_allocate_lpage(txn, pages)
      }
      None => {
        // Find or create a larger page.
        let larger_page_dev_offset = self.allocate_page(txn, pages, page_size_pow2 + 1).await;
        // Split the larger page in two, and release right page (we'll take the left one).
        let right_page_dev_offset = larger_page_dev_offset + (1 << page_size_pow2);
        self.insert_page_into_free_list(txn, pages, right_page_dev_offset, page_size_pow2);
        larger_page_dev_offset
      }
    }
  }

  pub async fn allocate_with_page_size(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    size: u64,
  ) -> (u64, u8) {
    let pow2 = max(
      pages.spage_size_pow2,
      size.next_power_of_two().ilog2().try_into().unwrap(),
    );
    assert!(pow2 <= pages.lpage_size_pow2);
    (self.allocate_page(txn, pages, pow2).await, pow2)
  }

  pub async fn allocate(&mut self, txn: &mut Transaction, pages: &Pages, size: u64) -> u64 {
    let (page_dev_offset, _) = self.allocate_with_page_size(txn, pages, size).await;
    page_dev_offset
  }

  #[async_recursion]
  pub async fn release(
    &mut self,
    txn: &mut Transaction,
    pages: &Pages,
    page_dev_offset: u64,
    page_size_pow2: u8,
  ) {
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < pages.lpage_size_pow2 {
      let buddy_page_dev_offset = page_dev_offset ^ (1 << page_size_pow2);
      if pages
        .check_if_page_is_free_page_of_size(buddy_page_dev_offset, page_size_pow2)
        .await
      {
        // Buddy is also free.
        // TODO Call pages.clear_page_header out of abundance of caution.
        self
          .release(
            txn,
            pages,
            page_dev_offset & !((1 << (page_size_pow2 + 1)) - 1),
            page_size_pow2 + 1,
          )
          .await;
        return;
      };
    };
    self.insert_page_into_free_list(txn, pages, page_dev_offset, page_size_pow2);
  }
}
