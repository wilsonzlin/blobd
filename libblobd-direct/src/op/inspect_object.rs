use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use chrono::DateTime;
use chrono::Utc;
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
  pub created: DateTime<Utc>,
}

pub(crate) async fn op_inspect_object(
  ctx: Arc<Ctx>,
  req: OpInspectObjectInput,
) -> OpResult<OpInspectObjectOutput> {
  let Some(obj) = ctx.committed_objects.get(&req.key).filter(|o| req.id.is_none() || Some(o.id()) == req.id).map(|e| e.value().clone()) else {
    return Err(OpError::ObjectNotFound);
  };
  Ok(OpInspectObjectOutput {
    id: obj.id(),
    size: obj.size,
    created: obj.created,
  })
}
