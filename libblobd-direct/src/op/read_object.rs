use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectState;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool_fixed::buf::FixedBuf;
use futures::Stream;
use off64::u32;
use off64::u64;
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
  pub data_stream: Pin<Box<dyn Stream<Item = OpResult<FixedBuf>> + Send>>,
  pub start: u64,
  pub end: u64,
  pub object_size: u64,
  pub object_id: u64,
}

fn object_is_still_valid(obj: &AutoLifecycleObject) -> OpResult<()> {
  if obj.get_state() == ObjectState::Committed {
    Ok(())
  } else {
    Err(OpError::ObjectNotFound)
  }
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  let Some(obj) = ctx.committed_objects.get(&req.key).filter(|o| req.id.is_none() || Some(o.id()) == req.id).map(|e| e.value().clone()) else {
    return Err(OpError::ObjectNotFound);
  };
  let object_id = obj.id();
  let object_size = obj.size();

  let start = req.start;
  // Exclusive.
  let end = req.end.unwrap_or(object_size);
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(OpError::RangeOutOfBounds);
  };

  let data_stream = async_stream::try_stream! {
    // This is the lpage index (incremented every lpage) or tail page index (incremented every tail page **which differ in size**).
    let mut idx = u32!(div_pow2(start, ctx.pages.lpage_size_pow2));
    if idx >= obj.lpage_count() {
      // We're starting inside the tail data, but that doesn't mean we're starting from the first tail page.
      // WARNING: Convert values to u64 BEFORE multiplying.
      let mut accum = u64!(idx) * u64!(obj.lpage_count());
      for (_, sz_pow2) in obj.tail_page_sizes() {
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
        if idx < obj.lpage_count() {
          let dev_offset = obj.lpage_dev_offset(idx);
          let page_size_pow2 = ctx.pages.lpage_size_pow2;
          (dev_offset, page_size_pow2)
        } else {
          let tail_idx = u8!(idx - obj.lpage_count());
          debug_assert!(tail_idx < obj.tail_page_count());
          let dev_offset = obj.tail_page_dev_offset(tail_idx);
          let page_size_pow2 = obj.tail_page_sizes().get(tail_idx).unwrap();
          (dev_offset, page_size_pow2)
        }
      };
      // The device offset of the current lpage or tail page changes each lpage amount, so this is not the same as `next`. Think of `next` as the virtual pointer within a contiguous span of the object's data bytes, and this as the physical offset within the physical page that backs the current position of the virtual pointer within the object's data made from many pages of different sizes.
      let offset_within_page = mod_pow2(next, page_size_pow2);

      // Can't read past current page, as we'll need to switch to a different page then.
      // TODO We could read in smaller amounts instead, to avoid higher memory usage from buffering.
      object_is_still_valid(&obj)?;
      let chunk_len = min(
        end - next,
        (1 << page_size_pow2) - offset_within_page,
      );
      trace!(idx, page_size_pow2, page_dev_offset, offset_within_page, chunk_len, start, next, end, "reading chunk");
      let data = ctx.device.read(page_dev_offset + offset_within_page, chunk_len).await;
      idx += 1;
      next += chunk_len;

      // Check again before yielding; we may have read junk.
      object_is_still_valid(&obj)?;
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
