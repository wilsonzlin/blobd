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

  // For crash consistency, delete any existing object first; otherwise, we may have two objects with the same key, and if versioning isn't enabled then it isn't clear which is the winner (objects can be committed in a different order than creation i.e. object ID).
  if let Some(existing) = ctx.committed_objects.insert(obj.key.clone(), obj) {
    // See `op_delete_object` for why this is safe to do now.
    reap_object(&ctx, &existing).await;
  };

  // Double check the state again, in case someone deleted it in the microseconds since we inserted it into `ctx.committed_objects`.
  ctx
    .tuples
    .update_object_state_if_exists_and_matches(
      object_id,
      ObjectState::Incomplete,
      ObjectState::Committed,
    )
    .await;

  Ok(OpCommitObjectOutput { object_id })
}
