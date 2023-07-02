use super::OpResult;
use crate::ctx::Ctx;
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
  Ok(OpDeleteObjectOutput {})
}
