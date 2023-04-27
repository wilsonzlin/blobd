use super::OpError;
use super::OpResult;
use super::UnalignedReader;
use crate::ctx::Ctx;
use crate::object::layout::calc_object_layout;
use crate::object::offset::OBJECT_OFF;
use crate::object::ObjectState;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use futures::Stream;
use off64::int::Off64AsyncReadInt;
use off64::u16;
use off64::u8;
use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tracing::trace;

pub struct OpReadObjectInput {
  pub key: TinyBuf,
  // Only useful if versioning is enabled.
  pub id: Option<u64>,
  pub start: u64,
  // Exclusive.
  pub end: Option<u64>,
}

pub struct OpReadObjectOutput {
  pub data_stream: Pin<Box<dyn Stream<Item = OpResult<Buf>> + Send>>,
  pub start: u64,
  pub end: u64,
  pub object_size: u64,
  pub object_id: u64,
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  let Some(bkt) = ctx.buckets.get_bucket_for_key(&req.key) else {
    return Err(OpError::ObjectNotFound);
  };

  let Some(obj_strong) = bkt.find_object(ctx.device.clone(), &req.key,  req.id).await else {
    return Err(OpError::ObjectNotFound);
  };
  let object_id = obj_strong.id;
  let object_size = obj_strong.size;
  let object_dev_offset = obj_strong.dev_offset;
  // We only need to check the state sometimes, so let the object drop in between those times; otherwise, we'll block all commits in the serial state worker loop if it does get deleted.
  let obj = obj_strong.into_weak();

  let start = req.start;
  // Exclusive.
  let end = req.end.unwrap_or(object_size);
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(OpError::RangeOutOfBounds);
  };

  let alloc_cfg = calc_object_layout(ctx.spage_size_pow2, ctx.lpage_size_pow2, object_size);

  let off = OBJECT_OFF
    .with_key_len(u16!(req.key.len()))
    .with_lpages(alloc_cfg.lpage_count)
    .with_tail_pages(alloc_cfg.tail_page_sizes_pow2.len());

  let unaligned_reader = UnalignedReader::new(ctx.device.clone(), ctx.spage_size_pow2);

  let data_stream = async_stream::try_stream! {
    // This is the lpage index (incremented every lpage) or tail page index (incremented every tail page **which differ in size**).
    let mut idx = div_pow2(start, ctx.lpage_size_pow2);
    if idx >= alloc_cfg.lpage_count {
      // We're starting inside the tail data, but that doesn't mean we're starting from the first tail page.
      let mut accum = idx * alloc_cfg.lpage_count;
      for (_, sz_pow2) in alloc_cfg.tail_page_sizes_pow2 {
        accum += 1 << sz_pow2;
        // This should be `>` not `>=`. For example, if lpage size is 16 MiB and first tail page is 8 MiB, and `start` is 24 MiB exactly, then it needs to start on the *second* tail page, not the first.
        if accum > start {
          break;
        };
        idx += 1;
      };
    };
    let mut next = start;
    while next < end {
      let (page_dev_offset, page_size_pow2) = {
        let _lock = obj.lock_if_still_valid(ObjectState::Committed)?;
        if idx < alloc_cfg.lpage_count {
          let dev_offset = unaligned_reader.read_u48_be_at(object_dev_offset + off.lpage(idx)).await;
          let page_size_pow2 = ctx.lpage_size_pow2;
          (dev_offset, page_size_pow2)
        } else {
          let tail_idx = u8!(idx - alloc_cfg.lpage_count);
          debug_assert!(tail_idx < alloc_cfg.tail_page_sizes_pow2.len());
          let dev_offset = unaligned_reader.read_u48_be_at(object_dev_offset + off.tail_page(tail_idx)).await;
          let page_size_pow2 = alloc_cfg.tail_page_sizes_pow2.get(tail_idx).unwrap();
          (dev_offset, page_size_pow2)
        }
      };
      // The device offset of the current lpage or tail page changes each lpage amount, so this is not the same as `next`. Think of `next` as the virtual pointer within a contiguous span of the object's data bytes, and this as the physical offset within the physical page that backs the current position of the virtual pointer within the object's data made from many pages of different sizes.
      let offset_within_page = mod_pow2(next, page_size_pow2);

      // Can't read past current page, as we'll need to switch to a different page then.
      // TODO We could read in smaller amounts instead, to avoid higher memory usage from buffering.
      let data = {
        let _lock = obj.lock_if_still_valid(ObjectState::Committed)?;
        let chunk_len = min(
          end - next,
          (1 << page_size_pow2) - offset_within_page,
        );
        trace!(idx, page_size_pow2, page_dev_offset, offset_within_page, chunk_len, start, next, end, "reading chunk");
        let data = ctx.device.read(page_dev_offset + offset_within_page, chunk_len).await;
        idx += 1;
        next += chunk_len;
        data
      };

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
