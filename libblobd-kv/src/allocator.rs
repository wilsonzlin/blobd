use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use crate::util::mul_pow2;
use off64::u64;
use off64::u8;
use off64::usz;
use roaring::RoaringTreemap;
use std::cmp::max;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::sync::atomic::Ordering::Relaxed;
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

type PageNum = u64;

struct PagesBitmap {
  base_dev_offset: u64,
  heap_size: u64,
  free: Vec<RoaringTreemap>, // One for each page size.
  pages: Pages,
}

impl PagesBitmap {
  fn bitmap(&self, page_size_pow2: u8) -> &RoaringTreemap {
    &self.free[usz!(page_size_pow2 - self.pages.spage_size_pow2)]
  }

  fn bitmap_mut(&mut self, page_size_pow2: u8) -> &mut RoaringTreemap {
    &mut self.free[usz!(page_size_pow2 - self.pages.spage_size_pow2)]
  }

  fn to_page_num(&self, page_dev_offset: u64, page_size_pow2: u8) -> PageNum {
    div_pow2(page_dev_offset - self.base_dev_offset, page_size_pow2)
  }

  fn to_page_dev_offset(&self, page_num: PageNum, page_size_pow2: u8) -> u64 {
    mul_pow2(page_num.into(), page_size_pow2) + self.base_dev_offset
  }

  fn get_free_page_with_min_size(&self, min_page_size_pow2: u8) -> Option<u64> {
    (min_page_size_pow2..=self.pages.lpage_size_pow2).find_map(|sz| {
      self
        .bitmap(sz)
        .min()
        .map(|page_no| self.to_page_dev_offset(page_no, sz))
    })
  }

  // Marks a single page as allocated, splitting any larger page as necessary.
  fn allocate(&mut self, page_num: PageNum, page_size_pow2: u8) {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    assert!(page_num < div_pow2(self.heap_size, page_size_pow2));
    if !self.bitmap_mut(page_size_pow2).remove(page_num) {
      // We need to split parent.
      self.allocate(page_num / 2, page_size_pow2 + 1);
      self.bitmap_mut(page_size_pow2).insert(page_num ^ 1);
    };
  }

  // Marks a single page as free, recursively merging as necessary.
  fn release(&mut self, page_num: PageNum, page_size_pow2: u8) {
    assert!(
      page_size_pow2 >= self.pages.spage_size_pow2 && page_size_pow2 <= self.pages.lpage_size_pow2
    );
    assert!(page_num < div_pow2(self.heap_size, page_size_pow2));
    // Check if buddy is also free so we can recompact. This doesn't apply to lpages as they don't have buddies (they aren't split).
    if page_size_pow2 < self.pages.lpage_size_pow2 {
      let buddy_page_num = page_num ^ 1;
      if self.bitmap(page_size_pow2).contains(buddy_page_num) {
        // Buddy is also free.
        assert!(self.bitmap_mut(page_size_pow2).remove(buddy_page_num));
        assert!(!self.bitmap(page_size_pow2).contains(page_num));
        // Merge by freeing parent larger page.
        let parent_page_num = page_num / 2;
        self.release(parent_page_num, page_size_pow2 + 1);
        return;
      };
    };
    assert!(self.bitmap_mut(page_size_pow2).insert(page_num));
  }
}

pub(crate) struct Allocator {
  bitmap: PagesBitmap,
  metrics: BlobdMetrics,
  pages: Pages,
}

impl Allocator {
  pub fn new(heap_dev_offset: u64, heap_size: u64, pages: Pages, metrics: BlobdMetrics) -> Self {
    assert_eq!(mod_pow2(heap_dev_offset, pages.lpage_size_pow2), 0);
    assert_eq!(mod_pow2(heap_size, pages.lpage_size_pow2), 0);
    Self {
      bitmap: PagesBitmap {
        base_dev_offset: heap_dev_offset,
        heap_size,
        free: (pages.spage_size_pow2..=pages.lpage_size_pow2)
          .map(|sz| {
            let page_count = div_pow2(heap_size, sz);
            let mut map = RoaringTreemap::new();
            if sz == pages.lpage_size_pow2 {
              map.insert_range(..page_count);
            };
            map
          })
          .collect(),
        pages: pages.clone(),
      },
      metrics,
      pages,
    }
  }

  pub fn mark_as_allocated(&mut self, page_dev_offset: u64, size: u32) {
    let alloc_size = ceil_pow2(u64!(size), self.pages.spage_size_pow2);
    let mut rem = alloc_size;
    let mut offset = page_dev_offset;
    while rem != 0 {
      let pos = 63u32.checked_sub(rem.leading_zeros()).unwrap();
      assert!(pos > 0);
      let page_size_pow2 = u8!(pos);
      self.bitmap.allocate(
        self.bitmap.to_page_num(offset, page_size_pow2),
        page_size_pow2,
      );
      offset += 1 << page_size_pow2;
      rem &= !(1 << pos);
    }
    self
      .metrics
      .0
      .allocated_bytes
      .fetch_add(alloc_size, Relaxed);
  }

  pub fn allocate(&mut self, size: u32) -> Result<u64, OutOfSpaceError> {
    // `contsize` is a power of two and greater than or equal to `size`. We find a free page that has a size of `contsize` or greater.
    let contsize = max(self.pages.spage_size(), u64!(size.next_power_of_two()));
    assert!(contsize <= self.pages.lpage_size());
    let dev_offset = self
      .bitmap
      .get_free_page_with_min_size(u8!(contsize.ilog2()))
      .ok_or(OutOfSpaceError)?;
    self.mark_as_allocated(dev_offset, size);
    Ok(dev_offset)
  }

  pub fn release(&mut self, page_dev_offset: u64, size: u32) {
    let mut alloc_size = ceil_pow2(u64!(size), self.pages.spage_size_pow2);
    let mut offset = page_dev_offset;
    while alloc_size != 0 {
      let pos = 63u32.checked_sub(alloc_size.leading_zeros()).unwrap();
      assert!(pos > 0);
      let page_size_pow2 = u8!(pos);
      self.bitmap.release(
        self.bitmap.to_page_num(offset, page_size_pow2),
        page_size_pow2,
      );
      offset += 1 << page_size_pow2;
      alloc_size &= !(1 << pos);
    }
    self
      .metrics
      .0
      .allocated_bytes
      .fetch_sub(alloc_size, Relaxed);
  }
}
