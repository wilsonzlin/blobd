use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use crate::util::mul_pow2;
use off64::u32;
use off64::u8;
use off64::usz;
use roaring::RoaringBitmap;
use std::cmp::max;
use std::sync::Arc;

#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum AllocDir {
  Left,
  Right,
}

type PageNum = u32;

pub(crate) struct Allocator {
  base_dev_offset: u64,
  dir: AllocDir,
  // One for each page size.
  free: Vec<RoaringBitmap>,
  metrics: Arc<BlobdMetrics>,
  pages: Pages,
}

impl Allocator {
  pub fn new(
    heap_dev_offset: u64,
    heap_size: u64,
    pages: Pages,
    dir: AllocDir,
    metrics: Arc<BlobdMetrics>,
  ) -> Self {
    assert_eq!(mod_pow2(heap_size, pages.lpage_size_pow2), 0);
    Self {
      base_dev_offset: heap_dev_offset,
      dir,
      free: (pages.spage_size_pow2..=pages.lpage_size_pow2)
        .map(|sz| {
          let page_count = u32!(div_pow2(heap_size, sz));
          let mut map = RoaringBitmap::new();
          if sz == pages.lpage_size_pow2 {
            map.insert_range(..page_count);
          };
          map
        })
        .collect(),
      pages,
      metrics,
    }
  }

  fn bitmap(&mut self, page_size_pow2: u8) -> &mut RoaringBitmap {
    &mut self.free[usz!(page_size_pow2 - self.pages.spage_size_pow2)]
  }

  fn to_page_num(&self, page_dev_offset: u64, page_size_pow2: u8) -> PageNum {
    u32!(div_pow2(
      page_dev_offset - self.base_dev_offset,
      page_size_pow2
    ))
  }

  fn to_page_dev_offset(&self, page_num: u32, page_size_pow2: u8) -> u64 {
    mul_pow2(page_num.into(), page_size_pow2) + self.base_dev_offset
  }

  fn _mark_as_allocated(&mut self, page_num: PageNum, page_size_pow2: u8) {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    if !self.bitmap(page_size_pow2).remove(page_num) {
      // We need to split parent.
      self._mark_as_allocated(page_num / 2, page_size_pow2 + 1);
      self.bitmap(page_size_pow2).insert(page_num ^ 1);
    };
  }

  pub fn mark_as_allocated(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    assert_eq!(mod_pow2(page_dev_offset, page_size_pow2), 0);

    self._mark_as_allocated(
      self.to_page_num(page_dev_offset, page_size_pow2),
      page_size_pow2,
    );

    self.metrics.incr_used_bytes(1 << page_size_pow2);
  }

  /// Returns the page number.
  fn allocate_page(&mut self, page_size_pow2: u8) -> PageNum {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    match if self.dir == AllocDir::Left {
      self.bitmap(page_size_pow2).min()
    } else {
      self.bitmap(page_size_pow2).max()
    } {
      Some(page_num) => {
        assert!(self.bitmap(page_size_pow2).remove(page_num));
        page_num
      }
      None if page_size_pow2 == self.pages.lpage_size_pow2 => {
        panic!("out of space");
      }
      None => {
        // Find or create a larger page.
        let larger_page_num = self.allocate_page(page_size_pow2 + 1);
        // Split the larger page in two, and release right page (we'll take the left one).
        assert!(self.bitmap(page_size_pow2).insert(larger_page_num * 2 + 1));
        larger_page_num * 2
      }
    }
  }

  pub fn allocate(&mut self, size: u64) -> u64 {
    let pow2 = max(
      self.pages.spage_size_pow2,
      u8!(size.next_power_of_two().ilog2()),
    );
    assert!(pow2 <= self.pages.lpage_size_pow2);
    // We increment these metrics here instead of in `Pages::*`, `Allocator::insert_into_free_list`, `Allocator::allocate_page`, etc. as many of those are called during intermediate states, like merging/splitting pages, which aren't actual allocations.
    self.metrics.incr_used_bytes(1 << pow2);
    let page_num = self.allocate_page(pow2);
    self.to_page_dev_offset(page_num, pow2)
  }

  fn release_internal(&mut self, page_num: PageNum, page_size_pow2: u8) {
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.pages.lpage_size_pow2 {
      let buddy_page_num = page_num ^ 1;
      if self.bitmap(page_size_pow2).contains(buddy_page_num) {
        // Buddy is also free.
        assert!(self.bitmap(page_size_pow2).remove(buddy_page_num));
        assert!(!self.bitmap(page_size_pow2).contains(page_num));
        // Merge by freeing parent larger page.
        let parent_page_num = page_num / 2;
        self.release_internal(parent_page_num, page_size_pow2 + 1);
        return;
      };
    };
    assert!(self.bitmap(page_size_pow2).insert(page_num));
  }

  pub fn release(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    let page_num = self.to_page_num(page_dev_offset, page_size_pow2);
    // Similar to `allocate`, we need to change metrics here and use an internal function. See `allocate` for comment explaining why.
    self.metrics.decr_used_bytes(1 << page_size_pow2);
    self.release_internal(page_num, page_size_pow2);
  }
}
