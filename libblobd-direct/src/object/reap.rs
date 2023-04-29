use super::layout::calc_object_layout;
use super::layout::ObjectLayout;
use super::offset::OBJECT_OFF;
use crate::allocator::Allocator;
use off64::int::Off64ReadInt;

#[must_use]
pub(crate) struct ReapedObject {
  pub object_metadata_size: u64,
  pub object_data_size: u64,
}

/// The object's metadata page should already be read, so we don't have to perform slow async I/O while holding state lock. This does essentially require optimistic read (i.e. acquire bucket read lock, find object, read metadata, upgrade to write lock and check it exists again).
pub(crate) fn reap_object(
  alloc: &mut Allocator,
  raw: &[u8],
  object_size: u64,
  spage_size_pow2: u8,
  lpage_size_pow2: u8,
  page_dev_offset: u64,
  page_size_pow2: u8,
) -> ReapedObject {
  let key_len = raw.read_u16_le_at(OBJECT_OFF.key_len());
  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(spage_size_pow2, lpage_size_pow2, object_size);

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len());
  for i in 0..lpage_count {
    let page_dev_offset = raw.read_u48_le_at(off.lpage(i));
    alloc.release(page_dev_offset, lpage_size_pow2);
  }
  for (i, tail_page_size_pow2) in tail_page_sizes_pow2 {
    let page_dev_offset = raw.read_u48_le_at(off.tail_page(i));
    alloc.release(page_dev_offset, tail_page_size_pow2);
  }
  alloc.release(page_dev_offset, page_size_pow2);
  ReapedObject {
    object_data_size: object_size,
    object_metadata_size: off._total_size(),
  }
}
