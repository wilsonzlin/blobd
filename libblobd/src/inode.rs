use crate::allocator::Allocator;
use crate::page::PagesMut;
use off64::usz;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::max;
use std::fmt::Debug;
use write_journal::Transaction;

/**

Structure
---------

We cannot store head data inline as we need to know the inode size in order to allocate a fragment for it, but we cannot know any inline head data length unless we know the inode size. For simplicity and flexibility, we just use a fragment for tail data, instead of storing it inline which would make it subject to the free space within the same fragmented tile.

The ordering of these fields is important (and somewhat strange/seemingly random), as we want to avoid multiple small reads.
- **write_object** requires `size`, `obj_id`, `key_len`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **find_inode_in_bucket** requires `obj_id`, `key_len`, `key`, and `next_inode_dev_offset_or_zero_if_end`.
- **read_object** requires *find_inode_in_bucket* as well as `size`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **commit_object** requires `obj_id`.

u40 size
u64 obj_id
u16 key_len
u8[] key
u48[] lpage_segment_page_dev_offset
u8 tail_segment_count
{
  u8 page_size_pow2
  u48 page_dev_offset
}[] tail_segments
u16 custom_metadata_byte_count
u16 custom_metadata_entry_count
{
  u16 name_len
  u16 value_len
  u8[] name
  u8[] value
}[] custom_metadata

**/

#[derive(Clone, Copy)]
pub(crate) struct InodeOffset;
impl InodeOffset {
  pub fn size() -> u64 {
    0
  }

  pub fn object_id() -> u64 {
    Self::size() + 5
  }

  pub fn key_len() -> u64 {
    Self::object_id() + 8
  }

  pub fn key() -> u64 {
    Self::key_len() + 2
  }

  pub fn with_key_len(key_len: u16) -> InodeOffsetFromKey {
    InodeOffsetFromKey {
      base: Self::key(),
      key_len,
    }
  }
}

#[derive(Clone, Copy)]
pub(crate) struct InodeOffsetFromKey {
  base: u64,
  key_len: u16,
}
impl InodeOffsetFromKey {
  pub fn lpage_segments(self) -> u64 {
    self.base + u64::from(self.key_len)
  }

  pub fn with_lpage_segments(self, lpage_segment_count: u64) -> InodeOffsetFromLpageSegments {
    InodeOffsetFromLpageSegments {
      base: self.lpage_segments(),
      lpage_segment_count,
    }
  }
}

#[derive(Clone, Copy)]
pub(crate) struct InodeOffsetFromLpageSegments {
  base: u64,
  lpage_segment_count: u64,
}
impl InodeOffsetFromLpageSegments {
  pub fn lpage_segment(self, idx: u64) -> u64 {
    self.base + 5 * idx
  }

  pub fn tail_segment_count(self) -> u64 {
    self.lpage_segment(self.lpage_segment_count)
  }

  pub fn tail_segments(self) -> u64 {
    self.tail_segment_count() + 1
  }

  pub fn with_tail_segments(self, tail_segment_count: u8) -> InodeOffsetFromTailSegments {
    InodeOffsetFromTailSegments {
      base: self.tail_segments(),
      tail_segment_count,
    }
  }
}

#[derive(Clone, Copy)]
pub(crate) struct InodeOffsetFromTailSegments {
  base: u64,
  tail_segment_count: u8,
}
impl InodeOffsetFromTailSegments {
  pub fn tail_segment(self, idx: u8) -> u64 {
    self.base + 6 * u64::from(idx)
  }

  pub fn custom_metadata_byte_count(self) -> u64 {
    self.tail_segment(self.tail_segment_count)
  }

  pub fn custom_metadata_entry_count(self) -> u64 {
    self.custom_metadata_byte_count() + 2
  }

  pub fn custom_metadata(self) -> u64 {
    self.custom_metadata_entry_count() + 2
  }

  pub fn with_custom_metadata(
    self,
    custom_metadata_byte_count: u16,
  ) -> InodeOffsetFromCustomMetadata {
    InodeOffsetFromCustomMetadata {
      base: self.custom_metadata(),
      custom_metadata_byte_count,
    }
  }
}

#[derive(Clone, Copy)]
pub(crate) struct InodeOffsetFromCustomMetadata {
  base: u64,
  custom_metadata_byte_count: u16,
}
impl InodeOffsetFromCustomMetadata {
  pub fn _total_inode_size(self) -> u64 {
    self.base + u64::from(self.custom_metadata_byte_count)
  }
}

// This makes it so that a read of the inode up to and including the key is at most exactly 512 bytes, which is a well-aligned well-sized no-waste read from most SSDs. In case you're worried that it's not long enough, this is 497 bytes: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.
pub const INO_KEY_LEN_MAX: u16 = 497;

/// WARNING: This does not verify the page type, but will clear the page header via `alloc.release`.
pub async fn free_inode(
  txn: &mut Transaction,
  dev: &SeekableAsyncFile,
  pages: &mut PagesMut,
  alloc: &mut Allocator,
  page_dev_offset: u64,
  page_size_pow2: u8,
) {
  let lpage_size_pow2 = pages.lpage_size_pow2;
  let raw = dev.read_at(page_dev_offset, 1 << page_size_pow2).await;
  let object_size = raw.read_u40_be_at(InodeOffset::size());
  let key_len = raw.read_u16_be_at(InodeOffset::key_len());
  let lpage_segment_count = object_size / (1 << pages.lpage_size_pow2);
  let off = InodeOffset::with_key_len(key_len).with_lpage_segments(lpage_segment_count);
  for i in 0..lpage_segment_count {
    let page_dev_offset = raw.read_u48_be_at(off.lpage_segment(i));
    alloc
      .release(txn, pages, page_dev_offset, lpage_size_pow2)
      .await;
  }
  let tail_segment_count = raw[usz!(off.tail_segment_count())];
  let off = off.with_tail_segments(tail_segment_count);
  for i in 0..tail_segment_count {
    let page_size_pow2 = raw[usz!(off.tail_segment(i))];
    let page_dev_offset = raw.read_u48_be_at(off.tail_segment(i) + 1);
    alloc
      .release(txn, pages, page_dev_offset, page_size_pow2)
      .await;
  }
  alloc
    .release(txn, pages, page_dev_offset, page_size_pow2)
    .await;
}

fn assert_is_strictly_descending<T: Debug + Ord>(vals: &[T]) {
  for i in 1..vals.len() {
    assert!(
      vals[i - 1] > vals[i],
      "element at index {i} is not strictly less than its previous element; list: {:?}",
      vals
    );
  }
}

pub fn calculate_object_tail_segments(
  spage_size: u64,
  lpage_size: u64,
  object_size: u64,
  max_segments: usize,
  fitness_factor: u64,
) -> Vec<u64> {
  // Initial invariant checks.
  assert!(max_segments >= 1);

  // Output.
  let mut segments = Vec::new();

  // Tail length.
  let mut rem = object_size % lpage_size;

  let max_frag = object_size / fitness_factor;
  loop {
    let mut page_size = max(spage_size, rem.next_power_of_two());
    // If we've reached the spage size, or we've run out of segments, or we've reached an acceptable internal fragmentation amount (even if we could reduce it more by using more unused segments), we can stop.
    if segments.len() == max_segments - 1 || page_size == spage_size || page_size - rem <= max_frag
    {
      segments.push(page_size);
      break;
    };
    page_size /= 2;
    segments.push(page_size);
    rem -= page_size;
  }

  // If we've reached the spage size, it's possible to push two spages, which is pointless and should be folded into one larger page. This may cause two of those page sizes, so keep folding until the last two page sizes aren't identical.
  while segments.len() >= 2 && segments.get(segments.len() - 1) == segments.get(segments.len() - 2)
  {
    let sz = segments.pop().unwrap();
    segments.pop().unwrap();
    segments.push(sz * 2);
  }

  // Extra final logic sanity checks.
  assert_is_strictly_descending(&segments);
  assert!(segments.len() <= max_segments);

  // Return output.
  segments
}

#[cfg(test)]
mod tests {
  use super::calculate_object_tail_segments;

  const SPAGE_SIZE: u64 = 512;
  const LPAGE_SIZE: u64 = 16777216;

  #[test]
  fn test_ff_100_mlu_10() {
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 0, 10, 100),
      vec![512],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 31, 10, 100),
      vec![512],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 511, 10, 100),
      vec![512],
    );
    // For 1000 bytes, MIF is 1000 / 100 = 10 bytes, using 1024 byte page would lead to 24 bytes waste, but 512 page is minimum.
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 1000, 10, 100),
      vec![1024],
    );
    // For 1014 bytes, MIF is 1014 / 100 = 10 bytes, using 1024 byte page would lead to 10 bytes waste.
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 1014, 10, 100),
      vec![1024],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 1535, 10, 100),
      vec![1024, 512],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 1537, 10, 100),
      vec![2048],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 65535, 10, 100),
      vec![65536],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 65537, 10, 100),
      vec![65536, 512],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 131071, 10, 100),
      vec![131072],
    );
  }

  #[test]
  fn test_ff_max_mlu_max() {
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 130560, usize::MAX, u64::MAX),
      vec![65536, 32768, 16384, 8192, 4096, 2048, 1024, 512],
    );
    assert_eq!(
      calculate_object_tail_segments(SPAGE_SIZE, LPAGE_SIZE, 130561, usize::MAX, u64::MAX),
      vec![131072],
    );
  }
}
