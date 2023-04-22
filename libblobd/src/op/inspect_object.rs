use super::OpError;
use super::OpResult;
use crate::bucket::FoundObject;
use crate::ctx::Ctx;
use std::sync::Arc;

pub struct OpInspectObjectInput {
  pub key: Vec<u8>,
}

pub struct OpInspectObjectOutput {
  pub id: u64,
  pub size: u64,
}

pub(crate) async fn op_inspect_object(
  ctx: Arc<Ctx>,
  req: OpInspectObjectInput,
) -> OpResult<OpInspectObjectOutput> {
  let bkt = ctx.buckets.get_bucket_for_key(&req.key).await;
  let Some(FoundObject { id, size, .. }) = bkt.find_object(None).await else {
    return Err(OpError::ObjectNotFound);
  };

  Ok(OpInspectObjectOutput { id, size })
}
