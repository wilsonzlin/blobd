use super::OpError;
use super::OpResult;
use crate::backing_store::PartitionStore;
use crate::ctx::Ctx;
use crate::object::calc_object_layout;
use crate::object::Object;
use crate::object::ObjectState;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::div_pow2;
use crate::util::floor_pow2;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use futures::Stream;
use off64::u32;
use off64::u64;
use off64::u8;
use off64::usz;
use std::cmp::max;
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

/// Both `offset` and `len` do not have to be multiples of the spage size.
async fn unaligned_read(pages: &Pages, dev: &PartitionStore, offset: u64, len: u64) -> Buf {
  let a_start = floor_pow2(offset, pages.spage_size_pow2);
  let a_end = max(
    ceil_pow2(offset + len, pages.spage_size_pow2),
    pages.spage_size(),
  );
  let mut buf = dev.read_at(a_start, a_end - a_start).await;
  buf.copy_within(usz!(offset - a_start)..usz!(offset - a_start + len), 0);
  buf.truncate(usz!(len));
  buf
}

fn object_is_still_valid(obj: &Object) -> OpResult<()> {
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
  let object_size = obj.size;
  let layout = calc_object_layout(&ctx.pages, object_size);
  let lpage_count = layout.lpage_count;
  let tail_page_count = layout.tail_page_sizes_pow2.len();

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
    if idx >= lpage_count {
      // We're starting inside the tail data, but that doesn't mean we're starting from the first tail page.
      // WARNING: Convert values to u64 BEFORE multiplying.
      let mut accum = u64!(idx) * ctx.pages.lpage_size();
      for (_, sz_pow2) in layout.tail_page_sizes_pow2 {
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
        if idx < lpage_count {
          let dev_offset = obj.lpage_dev_offsets[usz!(idx)];
          let page_size_pow2 = ctx.pages.lpage_size_pow2;
          (dev_offset, page_size_pow2)
        } else {
          let tail_idx = u8!(idx - lpage_count);
          assert!(tail_idx < tail_page_count);
          let dev_offset = obj.tail_page_dev_offsets[usz!(tail_idx)];
          let page_size_pow2 = layout.tail_page_sizes_pow2.get(tail_idx).unwrap();
          (dev_offset, page_size_pow2)
        }
      };
      // The device offset of the current lpage or tail page changes each lpage amount, so this is not the same as `next`. Think of `next` as the virtual pointer within a contiguous span of the object's data bytes, and this as the physical offset within the physical page that backs the current position of the virtual pointer within the object's data made from many pages of different sizes.
      let offset_within_page = mod_pow2(next, page_size_pow2);

      // Can't read past current page, as we'll need to switch to a different page then.
      // TODO We could read in smaller amounts instead, to avoid higher memory usage from buffering.
      let chunk_len = min(
        end - next,
        (1 << page_size_pow2) - offset_within_page,
      );
      trace!(idx, page_size_pow2, page_dev_offset, offset_within_page, chunk_len, start, next, end, "reading chunk");
      object_is_still_valid(&obj)?;
      let data = unaligned_read(&ctx.pages, &ctx.device, page_dev_offset + offset_within_page, chunk_len).await;
      assert_eq!(u64!(data.len()), chunk_len);
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
