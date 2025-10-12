#[cfg(test)]
pub mod tests;

use crate::metrics::BlobdMetrics;
use crate::page::Pages;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use crate::util::mul_pow2;
use off64::u32;
use off64::u8;
use off64::usz;
use roaring::RoaringBitmap;
use std::cmp::max;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use struct_name::StructName;
use struct_name_macro::StructName;

#[derive(PartialEq, Eq, Clone, Copy, Debug, StructName)]
pub(crate) struct OutOfSpaceError;

impl Display for OutOfSpaceError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(Self::struct_name())
  }
}

impl Error for OutOfSpaceError {}

#[allow(unused)]
#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum AllocDir {
  Left,
  Right,
}

type PageNum = u32;

#[derive(Default)]
pub(crate) struct Allocations(Vec<(u64, u8)>); // (offset, size_pow2)

impl Allocations {
  pub fn new() -> Self {
    Self(Vec::new())
  }

  pub fn add(&mut self, offset: u64, size_pow2: u8) {
    self.0.push((offset, size_pow2));
  }
}

pub(crate) struct Allocator {
  base_dev_offset: u64,
  dir: AllocDir,
  // One for each page size.
  free: Vec<RoaringBitmap>,
  pages: Pages,
  metrics: Arc<BlobdMetrics>,
}

impl Allocator {
  pub fn new(
    heap_dev_offset: u64,
    heap_size: u64,
    pages: Pages,
    dir: AllocDir,
    metrics: Arc<BlobdMetrics>,
  ) -> Self {
    assert_eq!(mod_pow2(heap_dev_offset, pages.lpage_size_pow2), 0);
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

  fn bitmap(&self, page_size_pow2: u8) -> &RoaringBitmap {
    &self.free[usz!(page_size_pow2 - self.pages.spage_size_pow2)]
  }

  fn bitmap_mut(&mut self, page_size_pow2: u8) -> &mut RoaringBitmap {
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
    if !self.bitmap_mut(page_size_pow2).remove(page_num) {
      // We need to split parent.
      self._mark_as_allocated(page_num / 2, page_size_pow2 + 1);
      self.bitmap_mut(page_size_pow2).insert(page_num ^ 1);
    };
  }

  pub fn mark_as_allocated(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    assert_eq!(mod_pow2(page_dev_offset, page_size_pow2), 0);

    self._mark_as_allocated(
      self.to_page_num(page_dev_offset, page_size_pow2),
      page_size_pow2,
    );

    self
      .metrics
      .allocated_bytes
      .fetch_add(1 << page_size_pow2, Relaxed);
  }

  /// Returns the page number.
  fn _allocate(&mut self, page_size_pow2: u8) -> Result<PageNum, OutOfSpaceError> {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    match if self.dir == AllocDir::Left {
      self.bitmap(page_size_pow2).min()
    } else {
      self.bitmap(page_size_pow2).max()
    } {
      Some(page_num) => {
        assert!(self.bitmap_mut(page_size_pow2).remove(page_num));
        Ok(page_num)
      }
      None if page_size_pow2 == self.pages.lpage_size_pow2 => Err(OutOfSpaceError),
      None => {
        // Find or create a larger page.
        let larger_page_num = self._allocate(page_size_pow2 + 1)?;
        // Split the larger page in two, and release left/right page depending on allocation direction (we'll take the other one).
        Ok(match self.dir {
          // We'll take the left page, so free the right one.
          AllocDir::Left => {
            assert!(self
              .bitmap_mut(page_size_pow2)
              .insert(larger_page_num * 2 + 1));
            larger_page_num * 2
          }
          // We'll take the right page, so free the left one.
          AllocDir::Right => {
            assert!(self.bitmap_mut(page_size_pow2).insert(larger_page_num * 2));
            larger_page_num * 2 + 1
          }
        })
      }
    }
  }

  pub fn allocate(&mut self, size: u64) -> Result<u64, OutOfSpaceError> {
    let pow2 = max(
      self.pages.spage_size_pow2,
      u8!(size.next_power_of_two().ilog2()),
    );
    assert!(pow2 <= self.pages.lpage_size_pow2);
    // We increment these metrics here instead of in `Pages::*`, `Allocator::insert_into_free_list`, `Allocator::allocate_page`, etc. as many of those are called during intermediate states, like merging/splitting pages, which aren't actual allocations.
    self.metrics.allocated_bytes.fetch_add(1 << pow2, Relaxed);
    let page_num = self._allocate(pow2)?;
    Ok(self.to_page_dev_offset(page_num, pow2))
  }

  fn _release(&mut self, page_num: PageNum, page_size_pow2: u8) {
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.pages.lpage_size_pow2 {
      let buddy_page_num = page_num ^ 1;
      if self.bitmap(page_size_pow2).contains(buddy_page_num) {
        // Buddy is also free.
        assert!(self.bitmap_mut(page_size_pow2).remove(buddy_page_num));
        assert!(!self.bitmap(page_size_pow2).contains(page_num));
        // Merge by freeing parent larger page.
        let parent_page_num = page_num / 2;
        self._release(parent_page_num, page_size_pow2 + 1);
        return;
      };
    };
    assert!(self.bitmap_mut(page_size_pow2).insert(page_num));
  }

  pub fn release(&mut self, page_dev_offset: u64, page_size_pow2: u8) {
    let page_num = self.to_page_num(page_dev_offset, page_size_pow2);
    // Similar to `allocate`, we need to change metrics here and use an internal function. See `allocate` for comment explaining why.
    self
      .metrics
      .allocated_bytes
      .fetch_sub(1 << page_size_pow2, Relaxed);
    self._release(page_num, page_size_pow2);
  }

  pub fn release_all(&mut self, to_free: &Allocations) {
    for (page_dev_offset, page_size_pow2) in &to_free.0 {
      self.release(*page_dev_offset, *page_size_pow2);
    }
  }
}
