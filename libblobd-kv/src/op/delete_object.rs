use super::OpResult;
use crate::ctx::Ctx;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpDeleteObjectInput {
  pub key: TinyBuf,
}

pub struct OpDeleteObjectOutput {}

pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  ctx.log_buffer.delete_tuple(req.key).await;
  ctx
    .metrics
    .0
    .delete_op_count
    .fetch_add(1, Ordering::Relaxed);
  Ok(OpDeleteObjectOutput {})
}
