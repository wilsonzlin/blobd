use tracing::instrument;

use super::OpError;
use super::OpResult;
use crate::bucket::FoundObject;
use crate::ctx::Ctx;
use crate::object::ObjectState;
use std::sync::Arc;

pub struct OpInspectObjectInput {
  pub key: Vec<u8>,
  // Only useful if versioning is enabled.
  pub id: Option<u128>,
}

pub struct OpInspectObjectOutput {
  pub id: u128,
  pub size: u64,
}

#[instrument(skip_all)]
pub(crate) async fn op_inspect_object(
  ctx: Arc<Ctx>,
  req: OpInspectObjectInput,
) -> OpResult<OpInspectObjectOutput> {
  let locker = ctx.buckets.get_locker_for_key(&req.key);
  let bkt = locker.read().await;
  let Some(FoundObject { meta, .. }) = bkt.find_object(ObjectState::Committed, req.id).await else {
    return Err(OpError::ObjectNotFound);
  };
  let id = meta.id();
  let size = meta.size();

  Ok(OpInspectObjectOutput { id, size })
}
