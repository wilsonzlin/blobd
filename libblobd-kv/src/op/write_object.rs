use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::object::ObjectTupleData;
use crate::object::OBJECT_SIZE_MAX;
use crate::object::OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD;
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

pub(crate) async fn op_write_object(
  ctx: Arc<Ctx>,
  req: OpWriteObjectInput,
) -> OpResult<OpWriteObjectOutput> {
  let size = u32!(req.data.len());

  if size > OBJECT_SIZE_MAX {
    return Err(OpError::ObjectTooLarge);
  };

  let tuple_data = if req.data.len() <= OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD {
    ObjectTupleData::Inline(req.data.into())
  } else {
    let Ok(dev_offset) = ctx.heap_allocator.lock().allocate(size) else {
      return Err(OpError::OutOfSpace);
    };
    let size_on_dev = max(
      ctx.pages.spage_size(),
      ceil_pow2(size.into(), ctx.pages.spage_size_pow2),
    );
    let padding = size_on_dev - u64!(size);
    let mut buf = ctx.pages.allocate_uninitialised(size_on_dev);
    buf.write_at(0, &req.data);
    ctx.device.write_at(dev_offset, buf).await;
    ctx
      .metrics
      .0
      .heap_object_data_bytes
      .fetch_add(size.into(), Relaxed);
    ctx
      .metrics
      .0
      .write_op_bytes_padding
      .fetch_add(padding, Relaxed);
    ObjectTupleData::Heap { size, dev_offset }
  };
  ctx.metrics.0.write_op_count.fetch_add(1, Relaxed);
  ctx
    .metrics
    .0
    .write_op_bytes_persisted
    .fetch_add(size.into(), Relaxed);

  ctx
    .bundles
    .upsert_tuple(ctx.clone(), req.key, tuple_data)
    .await;

  // TODO Is this necessary?
  // TODO Batch/delay.
  ctx.device.sync().await;

  Ok(OpWriteObjectOutput {})
}
