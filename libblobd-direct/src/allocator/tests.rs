use super::AllocDir;
use super::Allocator;
use crate::allocator::OutOfSpaceError;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use itertools::Itertools;
use off64::u32;
use off64::u8;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use rustc_hash::FxHashMap;
use std::cmp::min;

#[test]
fn test_allocator_from_left() {
  let spage_size_pow2 = 9;
  let lpage_size_pow2 = 14;
  let lpage_size = 1 << lpage_size_pow2;
  let heap_dev_offset = lpage_size * 31;
  let heap_size = lpage_size * 7;
  let pages = Pages::new(spage_size_pow2, lpage_size_pow2);

  let mut alloc = Allocator::new(
    heap_dev_offset,
    heap_size,
    pages,
    AllocDir::Left,
    BlobdMetrics::default(),
  );
  assert!(alloc.bitmap(9).is_empty());
  assert!(alloc.bitmap(10).is_empty());
  assert!(alloc.bitmap(11).is_empty());
  assert!(alloc.bitmap(12).is_empty());
  assert!(alloc.bitmap(13).is_empty());
  assert!(alloc.bitmap(14).contains_range(0..7));

  // Allocate all space.
  let mut seen = FxHashMap::default();
  let mut free = heap_size;
  while free > 0 {
    // We must always use `free.ilog2()`, as otherwise we may attempt to allocate rounded up to next page size that is greater than `free`.
    let sz_log2 = min(
      u8!(free.ilog2()),
      thread_rng().gen_range(spage_size_pow2..=lpage_size_pow2),
    );
    let sz = 1 << sz_log2;
    let page_dev_offset = alloc.allocate(sz).unwrap();
    assert_eq!(page_dev_offset % sz, 0);
    assert!(
      page_dev_offset >= heap_dev_offset,
      "got offset before heap (got {})",
      page_dev_offset
    );
    assert!(
      page_dev_offset + sz <= heap_dev_offset + heap_size,
      "got offset outside of heap (got {})",
      page_dev_offset
    );
    let None = seen.insert(page_dev_offset, sz_log2) else {
      panic!("repeated return value");
    };
    free -= sz;
  }
  assert_eq!(alloc.allocate(1).err(), Some(OutOfSpaceError));

  // Release all pages.
  let mut to_release = seen.iter().map(|(k, v)| (*k, *v)).collect_vec();
  to_release.shuffle(&mut thread_rng());
  for (page_dev_offset, page_size_pow2) in to_release {
    assert!(!alloc
      .bitmap(page_size_pow2)
      .contains(u32!(page_dev_offset / (1 << page_size_pow2))));
    alloc.release(page_dev_offset, page_size_pow2);
  }
  assert!(alloc.bitmap(9).is_empty());
  assert!(alloc.bitmap(10).is_empty());
  assert!(alloc.bitmap(11).is_empty());
  assert!(alloc.bitmap(12).is_empty());
  assert!(alloc.bitmap(13).is_empty());
  assert!(alloc.bitmap(14).contains_range(0..7));
}

#[test]
fn test_allocator_from_right() {
  let spage_size_pow2 = 9;
  let lpage_size_pow2 = 14;
  let lpage_size = 1 << lpage_size_pow2;
  let heap_dev_offset = lpage_size * 31;
  let heap_size = lpage_size * 7;
  let pages = Pages::new(spage_size_pow2, lpage_size_pow2);

  let mut alloc = Allocator::new(
    heap_dev_offset,
    heap_size,
    pages,
    AllocDir::Right,
    BlobdMetrics::default(),
  );

  // Allocate all space.
  let mut last_seen_for_size = FxHashMap::default();
  let mut seen = FxHashMap::default();
  let mut free = heap_size;
  while free > 0 {
    // We must always use `free.ilog2()`, as otherwise we may attempt to allocate rounded up to next page size that is greater than `free`.
    let sz_pow2 = min(
      u8!(free.ilog2()),
      thread_rng().gen_range(spage_size_pow2..=lpage_size_pow2),
    );
    let sz = 1 << sz_pow2;
    let page_dev_offset = alloc.allocate(sz).unwrap();
    assert_eq!(page_dev_offset % sz, 0);
    assert!(
      page_dev_offset >= heap_dev_offset,
      "got offset before heap (got {})",
      page_dev_offset
    );
    assert!(
      page_dev_offset + sz <= heap_dev_offset + heap_size,
      "got offset outside of heap (got {})",
      page_dev_offset
    );
    if let Some(last_seen) = last_seen_for_size
      .insert(sz, page_dev_offset)
      .filter(|p| *p <= page_dev_offset)
    {
      panic!(
        "repeated or increasing return value (last seen {}, got {})",
        last_seen, page_dev_offset
      );
    };
    let None = seen.insert(page_dev_offset, sz_pow2) else {
      unreachable!();
    };
    free -= sz;
  }
  assert_eq!(alloc.allocate(1).err(), Some(OutOfSpaceError));

  // Release all pages.
  let mut to_release = seen.iter().map(|(k, v)| (*k, *v)).collect_vec();
  to_release.shuffle(&mut thread_rng());
  for (page_dev_offset, page_size_pow2) in to_release {
    assert!(!alloc
      .bitmap(page_size_pow2)
      .contains(u32!(page_dev_offset / (1 << page_size_pow2))));
    alloc.release(page_dev_offset, page_size_pow2);
  }
  assert!(alloc.bitmap(9).is_empty());
  assert!(alloc.bitmap(10).is_empty());
  assert!(alloc.bitmap(11).is_empty());
  assert!(alloc.bitmap(12).is_empty());
  assert!(alloc.bitmap(13).is_empty());
  assert!(alloc.bitmap(14).contains_range(0..7));
}
