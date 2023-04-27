use crate::op::delete_object::OpDeleteObjectOutput;
use crate::op::OpError;
use crate::op::OpResult;
use crate::state::State;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use crate::util::get_now_ms;
use tinybuf::TinyBuf;

pub(crate) struct ActionDeleteObjectInput {
  pub bucket_id: u64,
  pub object_id: u64,
  pub key: TinyBuf,
}

pub(crate) fn action_delete_object(
  state: &mut State,
  req: ActionDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let ActionDeleteObjectInput {
    bucket_id,
    object_id,
    key,
  } = req;

  let Some(obj) = state.buckets.get_or_create_bucket_mut_by_id(bucket_id).update_list(None, Some(object_id)) else {
    // This could happen as our initial lookup is optimistic.
    return Err(OpError::ObjectNotFound);
  };

  let now = get_now_ms();

  // If the object isn't reaped during the next transaction commit, the event will be invalid and our `Ok` response to the user is not actually correct.
  obj.delete();

  state.stream.write().add_event_pending_commit(StreamEvent {
    typ: StreamEventType::ObjectDelete,
    timestamp_ms: now,
    object_id,
    key,
  });

  Ok(OpDeleteObjectOutput {})
}
