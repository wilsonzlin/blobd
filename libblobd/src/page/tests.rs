use super::Pages;
use crate::test_util::device::TestSeekableAsyncFile;
use crate::test_util::journal::TestWriteJournal;
use std::sync::Arc;

fn pow2(v: u8) -> u64 {
  2u64.checked_pow(v.into()).unwrap()
}

#[test]
fn test_get_page_free_bit_offset() {
  let lpage_size_pow2 = 21;
  let spage_size_pow2 = 8;
  let lpage_size = pow2(lpage_size_pow2);
  let spage_size = pow2(spage_size_pow2);
  let pages_per_lpage = 2 * (lpage_size / spage_size);
  // We have 2^L * 8 bits in a lpage (where L is log2(lpage size)), and we have `pages_per_lpage` pages per lpage.
  let lpages_per_block = lpage_size * 8 / pages_per_lpage;
  let block_size = lpages_per_block * lpage_size;

  // `17` is an arbitrary value. For testing, don't choose a power of 2, and prefer a prime number.
  let heap_dev_offset = lpage_size * 17;
  // `37` is an arbitrary value. For testing, don't choose a power of 2, and prefer a prime number.
  let block_dev_offset = heap_dev_offset + (block_size * 37);

  dbg!(block_size, heap_dev_offset, block_dev_offset);

  let dev = TestSeekableAsyncFile::new();
  let journal = Arc::new(TestWriteJournal::new(dev));
  let pages = Pages::new(journal, heap_dev_offset, spage_size_pow2, lpage_size_pow2);

  let mut highest_seen = (0, 0);
  // The first lpage is always reserved for the bitmap.
  for lpage_no_in_block in 1..lpages_per_block {
    // Use counter to be extra certain, as any calculation based on a formula may be wrong.
    let mut counter = 0;
    for page_size_pow2 in (spage_size_pow2..=lpage_size_pow2).rev() {
      let page_size = pow2(page_size_pow2);
      let pages_in_lpage = lpage_size / page_size;
      for page_no_in_lpage in 0..pages_in_lpage {
        let page_dev_offset =
          block_dev_offset + (lpage_no_in_block * lpage_size) + (page_no_in_lpage * page_size);
        let res = pages.get_page_free_bit_offset(page_dev_offset, page_size_pow2);
        let expected_id = (lpage_no_in_block * pages_per_lpage) + counter;
        // WARNING: `int_val / 64 * 8` is NOT the same as `int_val / 8` due to floor division.
        assert_eq!(res.0, block_dev_offset + (expected_id / 64 * 8), "64-bit bitmap element dev offset for page {page_no_in_lpage} of size {page_size} in lpage {lpage_no_in_block}");
        assert_eq!(res.1, expected_id % 64);

        // Extra character checks: element dev offset should be in bitmap, be a multiple of a 64-bit element address, etc.
        assert!((res.0 - heap_dev_offset) % block_size < lpage_size);
        assert_eq!((res.0 - block_dev_offset) % 8, 0);
        assert!(res.1 < 64);

        // Extra sanity check: make sure all return values are unique.
        assert!(highest_seen < res);
        highest_seen = res;
        counter += 1;
      }
    }
  }
}
