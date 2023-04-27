use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpInspectObjectInput {
  pub key: TinyBuf,
  // Only useful if versioning is enabled.
  pub id: Option<u64>,
}

pub struct OpInspectObjectOutput {
  pub id: u64,
  pub size: u64,
}

pub(crate) async fn op_inspect_object(
  ctx: Arc<Ctx>,
  req: OpInspectObjectInput,
) -> OpResult<OpInspectObjectOutput> {
  let Some(bkt) = ctx.buckets.get_bucket_for_key(&req.key) else {
    return Err(OpError::ObjectNotFound);
  };
  let Some(obj) = bkt.find_object(ctx.device.clone(), &req.key, req.id).await else {
    return Err(OpError::ObjectNotFound);
  };
  Ok(OpInspectObjectOutput {
    id: obj.id,
    size: obj.size,
  })
}
