pub mod bitmap;

use self::bitmap::BitmapContainers;
use crate::journal::Transaction;
use crate::metrics::BlobdMetrics;
use crate::uring::Uring;
use crate::util::div_ceil;
use crate::util::div_pow2;
use crate::util::floor_pow2;
use crate::util::mod_pow2;
use crate::util::mul_pow2;
use bufpool::BUFPOOL;
use off64::int::Off64WriteMutInt;
use off64::usz;
use std::cmp::max;
use std::cmp::min;
use std::sync::Arc;
use tokio::spawn;
use tracing::debug;
use tracing::trace;

pub(crate) struct Allocator {
  // One for each page size.
  free: Vec<BitmapContainers>,
  metrics: Arc<BlobdMetrics>,
  spage_size_pow2: u8,
  lpage_size_pow2: u8,
}

impl Allocator {
  pub async fn load_from_device(
    dev: Uring,
    state_dev_offset: u64,
    heap_dev_offset: u64,
    spage_size_pow2: u8,
    lpage_size_pow2: u8,
    metrics: Arc<BlobdMetrics>,
  ) -> Self {
    // Getting the buddy of a page using only XOR requires that the heap starts at an address aligned to the lpage size.
    assert_eq!(mod_pow2(heap_dev_offset, lpage_size_pow2), 0);
    // The first bitmap containers for all page sizes are stored sequentially from `state_dev_offset`.
    let spage_size = 1 << spage_size_pow2;
    let mut free = Vec::new();
    for page_size_pow2 in spage_size_pow2..=lpage_size_pow2 {
      free.push(
        BitmapContainers::load_from_device(
          dev.clone(),
          spage_size,
          page_size_pow2,
          heap_dev_offset,
          state_dev_offset + u64::from(page_size_pow2) * spage_size,
        )
        .await,
      );
    }
    debug!("allocator loaded");
    Self {
      free,
      lpage_size_pow2,
      metrics,
      spage_size_pow2,
    }
  }

  // Calculate how many spages will need to be allocated to store bitmap containers.
  // NOTE: This is slightly inefficient because it doesn't exclude reserved space, but we consider initial bitmap container pages as part of the reserved space, so it's a chicken-and-egg problem.
  pub fn initial_bitmap_container_page_count(
    dev_size: u64,
    spage_size_pow2: u8,
    lpage_size_pow2: u8,
  ) -> u64 {
    let _page_sizes = u64::from(lpage_size_pow2 - spage_size_pow2 + 1);
    let spage_size = 1 << spage_size_pow2;
    // One block contains one serialised bitmap container per page size.
    let bits_per_spage = (spage_size - 8) * 8;

    let mut spages_used_by_containers = 0;
    for page_size_pow2 in spage_size_pow2..=lpage_size_pow2 {
      let page_count = div_pow2(dev_size, page_size_pow2);
      spages_used_by_containers += div_ceil(page_count, bits_per_spage);
    }
    spages_used_by_containers
  }

  /// See README.md for layout.
  pub async fn format_device(
    dev: Uring,
    dev_size: u64,
    spage_size_pow2: u8,
    lpage_size_pow2: u8,
    state_dev_offset: u64,
  ) {
    // For calculation simplicity and correctness, the device size must be a multiple of the lpage size, which should almost always be true.
    assert_eq!(mod_pow2(dev_size, lpage_size_pow2), 0);

    let page_sizes = u64::from(lpage_size_pow2 - spage_size_pow2 + 1);
    let spage_size = 1 << spage_size_pow2;
    // One block contains one serialised bitmap container per page size.
    let block_size = page_sizes * spage_size;
    let bits_per_spage = (spage_size - 8) * 8;

    // Create and write bitmap containers.
    let mut write_tasks = Vec::new();
    for page_size_pow2 in spage_size_pow2..=lpage_size_pow2 {
      let page_count = div_pow2(dev_size, page_size_pow2);
      for i in 0..page_count / bits_per_spage {
        // What page number of size `page_size_pow2` this bitmap container represents from.
        let page_num_base = i * bits_per_spage;
        // What page number of size `page_size_pow2` this bitmap container represents to (exclusive).
        let page_num_end = min(page_count, page_num_base + bits_per_spage);
        // Device offset of this bitmap container.
        let dev_offset = state_dev_offset
          + (block_size * i)
          + (spage_size * u64::from(page_size_pow2 - spage_size_pow2));
        // Device offset of the next bitmap container.
        let next_dev_offset = if page_num_end == page_count {
          0
        } else {
          state_dev_offset + (block_size * i)
        };
        let mut buf = BUFPOOL.allocate_with_zeros(usz!(spage_size));
        buf[..usz!(spage_size - 8)].fill(0xff);
        buf.write_u64_le_at(spage_size - 8, next_dev_offset);
        write_tasks.push(spawn({
          let dev = dev.clone();
          async move {
            dev.write(dev_offset, buf).await;
          }
        }));
      }
    }
    for t in write_tasks {
      t.await.unwrap();
    }
  }

  pub fn commit(&mut self, txn: &mut Transaction) {
    for bc in self.free.iter_mut() {
      bc.commit(txn);
    }
  }

  fn bitmap(&mut self, page_size_pow2: u8) -> &mut BitmapContainers {
    &mut self.free[usz!(page_size_pow2 - self.spage_size_pow2)]
  }

  fn allocate_page(&mut self, page_size_pow2: u8) -> u64 {
    assert!(page_size_pow2 >= self.spage_size_pow2 && page_size_pow2 <= self.lpage_size_pow2);
    let page_dev_offset = match self.bitmap(page_size_pow2).pop_free_page() {
      Some(page_num) => mul_pow2(page_num, page_size_pow2),
      None if page_size_pow2 == self.lpage_size_pow2 => {
        panic!("out of space");
      }
      None => {
        trace!(
          page_size_pow2,
          "ran out of pages, will allocate page of the next bigger size"
        );
        // Find or create a larger page.
        let larger_page_dev_offset = self.allocate_page(page_size_pow2 + 1);
        // Split the larger page in two, and release right page (we'll take the left one).
        let right_page_dev_offset = larger_page_dev_offset + (1 << page_size_pow2);
        self
          .bitmap(page_size_pow2)
          .mark_page_as_free(right_page_dev_offset);
        larger_page_dev_offset
      }
    };
    // We must always mark an allocated page as not free, including split pages which definitely cannot be released/merged.
    self
      .bitmap(page_size_pow2)
      .mark_page_as_not_free(page_dev_offset);
    trace!(page_size_pow2, page_dev_offset, "allocated page");
    page_dev_offset
  }

  pub fn allocate(&mut self, size: u64) -> u64 {
    let pow2 = max(
      self.spage_size_pow2,
      size.next_power_of_two().ilog2().try_into().unwrap(),
    );
    assert!(pow2 <= self.lpage_size_pow2);
    // We increment these metrics here instead of in `Pages::*`, `Allocator::insert_into_free_list`, `Allocator::allocate_page`, etc. as many of those are called during intermediate states, like merging/splitting pages, which aren't actual allocations.
    self.metrics.incr_used_bytes(1 << pow2);
    self.allocate_page(pow2)
  }

  fn release_internal(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.lpage_size_pow2 {
      let buddy_page_dev_offset = page_dev_offset ^ (1 << page_size_pow2);
      if self
        .bitmap(page_size_pow2)
        .check_if_page_is_free(buddy_page_dev_offset)
      {
        // Buddy is also free.
        trace!(
          page_dev_offset,
          page_size_pow2,
          buddy_page_dev_offset,
          "buddy is also free"
        );
        self
          .bitmap(page_size_pow2)
          .mark_page_as_not_free(page_dev_offset);
        self
          .bitmap(page_size_pow2)
          .mark_page_as_not_free(buddy_page_dev_offset);
        // Merge by freeing parent larger page.
        let parent_page_dev_offset = floor_pow2(page_dev_offset, page_size_pow2 + 1);
        self.release_internal(parent_page_dev_offset, page_size_pow2 + 1);
        return;
      };
    };
    self
      .bitmap(page_size_pow2)
      .mark_page_as_free(page_dev_offset);
  }

  pub fn release(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    // Similar to `allocate_and_ret_with_size`, we need to change metrics here and use an internal function. See `allocate_and_ret_with_size` for comment explaining why.
    self.metrics.decr_used_bytes(1 << page_size_pow2);
    self.release_internal(page_dev_offset, page_size_pow2);
  }
}
