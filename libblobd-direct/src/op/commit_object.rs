use super::delete_object::reap_object;
use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::ObjectState;
use std::sync::atomic::Ordering;
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

  obj
    .update_state_then_ensure_no_writers(ObjectState::Committed)
    .await;

  // For crash consistency, we must generate a new object ID (such that it's greater than any existing incomplete or committed object's ID) for the object we're about to commit; otherwise, we may have two objects with the same key (e.g. we crash before we manage to delete the existing committed one), and if versioning isn't enabled then it isn't clear which is the winner (objects can be committed in a different order than creation).
  let new_object_id = ctx.next_object_id.fetch_add(1, Ordering::Relaxed);
  let existing = ctx
    .committed_objects
    .insert(obj.key.clone(), obj.with_new_id(new_object_id));

  ctx
    .tuples
    .update_object_id_and_state(
      req.incomplete_token.object_id,
      new_object_id,
      ObjectState::Committed,
    )
    .await;

  // Only delete AFTER we're certain the updated object tuple we're committing has been persisted to disk.
  // See `op_delete_object` for why this is safe to do at any time for a committed object.
  if let Some(existing) = existing.as_ref() {
    reap_object(&ctx, &existing).await;
  };

  ctx
    .metrics
    .0
    .commit_op_count
    .fetch_add(1, Ordering::Relaxed);
  ctx
    .metrics
    .0
    .incomplete_object_count
    .fetch_sub(1, Ordering::Relaxed);
  if existing.is_none() {
    ctx
      .metrics
      .0
      .committed_object_count
      .fetch_add(1, Ordering::Relaxed);
  };

  Ok(OpCommitObjectOutput {
    object_id: new_object_id,
  })
}
