use crate::incomplete_token::IncompleteToken;
use crate::op::commit_object::OpCommitObjectOutput;
use crate::op::OpError;
use crate::op::OpResult;
use crate::state::State;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use crate::util::get_now_ms;
use tinybuf::TinyBuf;

pub(crate) struct ActionCommitObjectInput {
  pub key: TinyBuf,
  pub incomplete_token: IncompleteToken,
  // Look up any existing object with the same key (which will be in the same bucket) before performing this action, as it will require async I/O. Yes, it's optimistic because someone could create one after we check but before this action is executed, but this is mostly a user error anyway as they should not be committing two identically-keyed objects simultaneously anyway.
  pub also_delete_object_in_same_bucket_with_id: Option<u64>,
}

pub(crate) fn action_commit_object(
  state: &mut State,
  req: ActionCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  assert!(state.cfg.versioning && req.also_delete_object_in_same_bucket_with_id.is_none());

  let IncompleteToken {
    object_id,
    bucket_id,
    ..
  } = req.incomplete_token;

  let Some(obj) = state.incomplete_list.remove(object_id) else {
    return Err(OpError::ObjectNotFound);
  };

  let now = get_now_ms();

  let deleted = state
    .buckets
    .get_or_create_bucket_mut_by_id(bucket_id)
    .update_list(Some(obj), req.also_delete_object_in_same_bucket_with_id);
  if let Some(deleted) = deleted {
    state.stream.write().add_event_pending_commit(StreamEvent {
      typ: StreamEventType::ObjectDelete,
      timestamp_ms: now,
      object_id: deleted.id,
      key: req.key.clone(),
    });
  };
  state.stream.write().add_event_pending_commit(StreamEvent {
    typ: StreamEventType::ObjectCommit,
    timestamp_ms: now,
    object_id,
    key: req.key,
  });

  Ok(OpCommitObjectOutput { object_id })
}
