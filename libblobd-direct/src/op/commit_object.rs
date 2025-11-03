use super::delete_object::reap_object;
use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::ObjectState;
use dashmap::mapref::entry::Entry;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct OpCommitObjectInput {
  pub incomplete_token: IncompleteToken,
  /// If true, the object will only be committed if there is no existing committed object with the same key; if there is an existing object, it will be left untouched, and this object will be deleted instead.
  pub if_not_exists: bool,
}

pub struct OpCommitObjectOutput {
  /// This will only be None if `if_not_exists` is true and an existing object with the same key was present.
  pub object_id: Option<u64>,
}

pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let Some(obj) = ctx
    .incomplete_objects
    .write()
    .remove(&req.incomplete_token.object_id)
  else {
    return Err(OpError::ObjectNotFound);
  };

  obj
    .update_state_then_ensure_no_writers(ObjectState::Committed)
    .await;

  let (to_delete, new_object_id) = match ctx.committed_objects.entry(obj.key.clone()) {
    Entry::Occupied(_) if req.if_not_exists => (Some(obj), None),
    e => {
      // For crash consistency, we must generate a new object ID (such that it's greater than any existing incomplete or committed object's ID) for the object we're about to commit; otherwise, we may have two objects with the same key (e.g. we crash before we manage to delete the existing committed one), and if versioning isn't enabled then it isn't clear which is the winner (objects can be committed in a different order than creation).
      let new_object_id = ctx.next_object_id.fetch_add(1, Ordering::Relaxed);
      let new_obj = obj.with_new_id(new_object_id);
      let existing = match e {
        Entry::Occupied(mut e) => Some(e.insert(new_obj)),
        Entry::Vacant(e) => {
          e.insert(new_obj);
          None
        }
      };
      ctx
        .tuples
        .update_object_id_and_state(
          req.incomplete_token.object_id,
          new_object_id,
          ObjectState::Committed,
        )
        .await;
      (existing, Some(new_object_id))
    }
  };

  // Only delete AFTER we're certain the updated object tuple we're committing has been persisted to disk.
  // See `op_delete_object` for why this is safe to do at any time for a committed object.
  if let Some(to_delete) = to_delete.as_ref() {
    reap_object(&ctx, to_delete).await;
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
  if new_object_id.is_some() && to_delete.is_none() {
    // We did the commit AND did NOT delete an existing object.
    ctx
      .metrics
      .0
      .committed_object_count
      .fetch_add(1, Ordering::Relaxed);
  }

  Ok(OpCommitObjectOutput {
    object_id: new_object_id,
  })
}
