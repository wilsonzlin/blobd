use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::calc_inode_layout;
use crate::inode::INODE_OFF;
use crate::op::key_debug_str;
use crate::page::PageType;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use futures::Stream;
use off64::int::Off64AsyncReadInt;
use off64::u16;
use off64::u8;
use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::trace;

pub struct OpReadObjectInput {
  pub key: Vec<u8>,
  pub start: u64,
  // Exclusive.
  pub end: Option<u64>,
  pub stream_buffer_size: u64,
}

pub struct OpReadObjectOutput {
  pub data_stream: Pin<Box<dyn Stream<Item = OpResult<Vec<u8>>> + Send>>,
  pub start: u64,
  pub end: u64,
  pub object_size: u64,
  pub object_id: u64,
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  trace!(
    key = key_debug_str(&req.key),
    start = req.start,
    end = req.end,
    "reading object"
  );

  // WARNING: Drop bucket lock immediately.
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = ctx.buckets.get_bucket_for_key(&req.key).await.find_inode(None).await else {
    return Err(OpError::ObjectNotFound);
  };
  let object_size = ctx.device.read_u40_be_at(INODE_OFF.size()).await;
  let start = req.start;
  // Exclusive.
  let end = req.end.unwrap_or(object_size);
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(OpError::RangeOutOfBounds);
  };
  trace!(
    key = key_debug_str(&req.key),
    inode_dev_offset,
    object_id,
    object_size,
    start,
    end,
    "found object to read"
  );

  let alloc_cfg = calc_inode_layout(&ctx.pages, object_size);
  let off = INODE_OFF
    .with_key_len(u16!(req.key.len()))
    .with_lpage_segments(alloc_cfg.lpage_segment_count)
    .with_tail_segments(alloc_cfg.tail_segment_page_sizes_pow2.len());

  let data_stream = async_stream::try_stream! {
    // This is the lpage index (incremented every lpage) or tail page index (incremented every tail page **which differ in size**).
    let mut idx = div_pow2(start, ctx.pages.lpage_size_pow2);
    let mut next = start;
    let mut last_checked_valid = Instant::now();
    while next < end {
      let now = Instant::now();
      if now.duration_since(last_checked_valid).as_secs() >= 60 {
        // Check that object is still valid.
        let (hdr_type, _) = ctx
          .pages
          .read_page_header_type_and_size(inode_dev_offset)
          .await;
        if hdr_type == PageType::ActiveInode {
          Ok(())
        } else {
          Err(OpError::ObjectNotFound)
        }?;
        last_checked_valid = now;
      }
      let (page_dev_offset, page_size_pow2) = if idx < alloc_cfg.lpage_segment_count {
        let dev_offset = ctx.device.read_u48_be_at(off.lpage_segment(idx)).await;
        let page_size_pow2 = ctx.pages.lpage_size_pow2;
        (dev_offset, page_size_pow2)
      } else {
        let tail_idx = u8!(idx - alloc_cfg.lpage_segment_count);
        let dev_offset = ctx.device.read_u48_be_at(off.tail_segment(tail_idx)).await;
        let page_size_pow2 = alloc_cfg.tail_segment_page_sizes_pow2.get(tail_idx).unwrap();
        (dev_offset, page_size_pow2)
      };
      // The device offset of the current lpage or tail page changes each lpage amount, so this is not the same as `next`. Think of `next` as the virtual pointer within a contiguous span of the object's data bytes, and this as the physical offset within the physical page that backs the current position of the virtual pointer within the object's data made from many pages of different sizes.
      let offset_within_page = mod_pow2(next, page_size_pow2);

      // Can't read past current page, as we'll need to switch to a different page then.
      // TODO We could read in smaller amounts instead, to avoid higher memory usage from buffering.
      let chunk_len = min(
        end - next,
        (1 << page_size_pow2) - offset_within_page,
      );
      let data = ctx.device.read_at(page_dev_offset + offset_within_page, chunk_len).await;
      idx += 1;
      next += chunk_len;
      yield data;
    };
  };

  Ok(OpReadObjectOutput {
    data_stream: Box::pin(data_stream),
    end,
    object_id,
    object_size,
    start,
  })
}
