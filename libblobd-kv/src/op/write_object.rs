use super::OpError;
use super::OpResult;
use crate::allocator::Allocator;
use crate::ctx::Ctx;
use crate::metrics::BlobdMetrics;
use crate::object::ObjectTupleData;
use crate::object::LOG_ENTRY_DATA_LEN_INLINE_THRESHOLD;
use crate::object::OBJECT_SIZE_MAX;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use off64::u32;
use off64::u64;
use off64::Off64WriteMut;
use std::cmp::max;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpWriteObjectInput {
  pub key: TinyBuf,
  pub data: Vec<u8>,
}

pub struct OpWriteObjectOutput {}

// This only updates the allocator and doesn't create the write buffer or actually perform the async write, so that we can efficiently bo a bunch of allocations without excessively acquiring and releasing the allocator lock.
// Returns the device offset and the padded size.
pub(crate) fn allocate_object_on_heap(
  heap_allocator: &mut Allocator,
  pages: &Pages,
  metrics: &BlobdMetrics,
  size: u32,
) -> Result<(u64, u64), OpError> {
  let Ok(dev_offset) = heap_allocator.allocate(size) else {
    return Err(OpError::OutOfSpace);
  };
  let size_on_dev = max(
    pages.spage_size(),
    ceil_pow2(size.into(), pages.spage_size_pow2),
  );
  let padding = size_on_dev - u64!(size);
  metrics
    .0
    .heap_object_data_bytes
    .fetch_add(size.into(), Relaxed);
  metrics.0.write_op_bytes_padding.fetch_add(padding, Relaxed);
  Ok((dev_offset, size_on_dev))
}

pub(crate) async fn op_write_object(
  ctx: Arc<Ctx>,
  req: OpWriteObjectInput,
) -> OpResult<OpWriteObjectOutput> {
  let size = req.data.len();
  if size > OBJECT_SIZE_MAX {
    return Err(OpError::ObjectTooLarge);
  };

  // This will go into the log first, so the threshold is not `OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD`.
  let tuple_data = if size <= LOG_ENTRY_DATA_LEN_INLINE_THRESHOLD {
    ObjectTupleData::Inline(req.data.into())
  } else {
    let (dev_offset, size_on_dev) = allocate_object_on_heap(
      &mut ctx.heap_allocator.lock(),
      &ctx.pages,
      &ctx.metrics,
      u32!(size),
    )?;
    let mut buf = ctx.pages.allocate_uninitialised(size_on_dev);
    buf.write_at(0, &req.data);
    ctx.device.write_at(dev_offset, buf).await;
    ObjectTupleData::Heap {
      size: u32!(size),
      dev_offset,
    }
  };
  ctx.metrics.0.write_op_count.fetch_add(1, Relaxed);
  ctx
    .metrics
    .0
    .write_op_bytes_persisted
    .fetch_add(u64!(size), Relaxed);

  ctx.log_buffer.upsert_tuple(req.key, tuple_data).await;

  Ok(OpWriteObjectOutput {})
}
