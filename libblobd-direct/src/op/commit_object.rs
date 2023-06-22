use super::delete_object::reap_object;
use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::ObjectState;
use std::sync::Arc;

pub struct OpCommitObjectInput {
  pub incomplete_token: IncompleteToken,
}

pub struct OpCommitObjectOutput {
  pub object_id: u64,
}

pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let Some(obj) = ctx.incomplete_objects.write().remove(&req.incomplete_token.object_id) else {
    return Err(OpError::ObjectNotFound);
  };
  let object_id = obj.id();

  obj
    .update_state_then_ensure_no_writers(ObjectState::Committed)
    .await;

  ctx
    .tuples
    .update_object_state(obj.id(), ObjectState::Committed)
    .await;

  // TODO Handle crash consistency: new object is committed but existing object isn't deleted.
  let existing = ctx.committed_objects.insert(obj.key.clone(), obj);
  if let Some(existing) = existing {
    // See `op_delete_object` for why this is safe to do now.
    reap_object(&ctx, &existing).await;
  };

  Ok(OpCommitObjectOutput { object_id })
}
