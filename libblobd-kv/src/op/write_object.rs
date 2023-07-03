use super::OpError;
use super::OpResult;
use crate::allocator::Allocator;
use crate::backing_store::BackingStore;
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
use parking_lot::Mutex;
use std::cmp::max;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpWriteObjectInput {
  pub key: TinyBuf,
  pub data: Vec<u8>,
}

pub struct OpWriteObjectOutput {}

pub(crate) async fn write_object_on_heap(
  dev: &Arc<dyn BackingStore>,
  heap_allocator: &Mutex<Allocator>,
  pages: &Pages,
  metrics: &BlobdMetrics,
  data: &[u8],
) -> Result<ObjectTupleData, OpError> {
  let size = u32!(data.len());
  let Ok(dev_offset) = heap_allocator.lock().allocate(size) else {
    return Err(OpError::OutOfSpace);
  };
  let size_on_dev = max(
    pages.spage_size(),
    ceil_pow2(size.into(), pages.spage_size_pow2),
  );
  let padding = size_on_dev - u64!(size);
  let mut buf = pages.allocate_uninitialised(size_on_dev);
  buf.write_at(0, data);
  dev.write_at(dev_offset, buf).await;
  metrics
    .0
    .heap_object_data_bytes
    .fetch_add(size.into(), Relaxed);
  metrics.0.write_op_bytes_padding.fetch_add(padding, Relaxed);
  Ok(ObjectTupleData::Heap { size, dev_offset })
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
    write_object_on_heap(
      &ctx.device,
      &ctx.heap_allocator,
      &ctx.pages,
      &ctx.metrics,
      &req.data,
    )
    .await?
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
