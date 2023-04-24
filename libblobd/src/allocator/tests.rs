use crate::allocator::Allocator;
use crate::metrics::BlobdMetrics;
use crate::page::Pages;
use crate::test_util::device::TestSeekableAsyncFile;
use crate::test_util::journal::TestWriteJournal;
use off64::usz;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test]
#[rustfmt::skip]
async fn test_allocation() {
  // For testing, don't just use nice power-of-2 values for arbitrary values.Prefer odd numbers, prime numbers, random numbers, etc.
  let alloc_dev_offset   = 1234;
  let metrics_dev_offset = 2551;
  let lpage_size_pow2    = 13; // 8192 bytes.
  let lpage_size         = 1 << lpage_size_pow2;
  let spage_size_pow2    = 8; // 256 bytes.
  let spage_size         = 1 << spage_size_pow2;
  let page_sizes         = lpage_size_pow2 - spage_size_pow2 + 1;
  let heap_dev_offset    = lpage_size * 2;
  let block_size         = (lpage_size * 8) / (2 * lpage_size / spage_size) * lpage_size;
  let dev_size           = Arc::new(AtomicU64::new(lpage_size * 16384 - 12345));

  let dev     = TestSeekableAsyncFile::new();
  let journal = Arc::new(TestWriteJournal::new(dev.clone()));
  let pages   = Arc::new(Pages::new(journal.clone(), heap_dev_offset, spage_size_pow2, lpage_size_pow2));
  let metrics = Arc::new(BlobdMetrics::for_testing(metrics_dev_offset));

  // Assert our calculations.
  assert_eq!(pages.block_size, block_size);

  // Set up allocator.
  Allocator::format_device(&dev, alloc_dev_offset, heap_dev_offset).await;
  let mut alloc = Allocator::load_from_device(
    &dev,
    dev_size,
    alloc_dev_offset,
    pages,
    metrics,
    heap_dev_offset,
  ).await;
  assert_eq!(alloc.state_dev_offset, alloc_dev_offset);
  assert_eq!(alloc.frontier_dev_offset, heap_dev_offset);
  assert_eq!(alloc.free_list_head, vec![0u64; usz!(page_sizes)]);

  // Aliased offsets.
  let blk0                 = heap_dev_offset + 0 * block_size;
  let _blk0_metadata_lpage = blk0 + 0 * lpage_size;
  let blk0_lpage1          = blk0 + 1 * lpage_size;
  let _blk0_lpage2         = blk0 + 2 * lpage_size;
  let blk1                 = heap_dev_offset + 1 * block_size;

  // First allocation: very small size, less than spage.
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 13).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1);
  let mut free_list_heads = vec![
    blk0_lpage1 +  256, // Page size  256. Page 0 was allocated, so next free is page 1.
    blk0_lpage1 +  512, // Page size  512. Page 0 was split, so next free is page 1.
    blk0_lpage1 + 1024, // Page size 1024. Page 0 was split, so next free is page 1.
    blk0_lpage1 + 2048, // Page size 2048. Page 0 was split, so next free is page 1.
    blk0_lpage1 + 4096, // Page size 4096. Page 0 was split, so next free is page 1.
    blk0_lpage1 + 8192, // Page size 8192. Page 0 was split, so next free is page 1.
  ];
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Second allocation: page size 1024 (log2 = 10).
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 1011).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 1 * 1024);
  free_list_heads[10 - 8] = 0;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Second allocation: page size 2048 (log2 = 11).
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 1025).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 1 * 2048);
  free_list_heads[11 - 8] = 0;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Third allocation: page size 1024 (log2 = 10).
  // There are no 2^10 pages available, so split 2^11 and use left page.
  // There are no 2^11 pages available, so split 2^12 and use left page.
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 713).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 2 * 2048);
  free_list_heads[12 - 8] = 0;
  free_list_heads[11 - 8] = blk0_lpage1 + 4096 + 2048;
  free_list_heads[10 - 8] = blk0_lpage1 + 4096 + 1024;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Fourth allocation: page size 256 (log2 = 8).
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 256).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 1 * 256);
  free_list_heads[8 - 8] = 0;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Fifth allocation: page size 256 (log2 = 8).
  // There are no 2^8 pages available, so split 2^9 and use left page.
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 0).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 1 * 512);
  free_list_heads[9 - 8] = 0;
  free_list_heads[8 - 8] = blk0_lpage1 + 512 + 256;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // Sixth allocation: page size 256 (log2 = 8).
  let mut txn = journal.begin_transaction();
  let page_dev_offset = alloc.allocate(&mut txn, 0).await;
  assert_eq!(alloc.frontier_dev_offset, blk1);
  assert_eq!(page_dev_offset, blk0_lpage1 + 3 * 256);
  free_list_heads[8 - 8] = 0;
  assert_eq!(alloc.free_list_head, free_list_heads);

  // TODO Test at least 2 more block allocations, then test out of space panic (ensure device size only fits 3.5 block sizes).
}
