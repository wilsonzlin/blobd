use super::delete_object::action_delete_object;
use super::delete_object::ActionDeleteObjectInput;
use crate::journal::Transaction;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectState;
use crate::op::commit_object::OpCommitObjectOutput;
use crate::op::OpResult;
use crate::state::State;
use crate::stream::StreamEventOwned;
use crate::stream::StreamEventType;
use crate::util::get_now_ms;
use tinybuf::TinyBuf;

pub(crate) struct ActionCommitObjectInput {
  /// WARNING: This object must be in the Committed state and have no writers (and will never have writers in the future).
  pub obj: AutoLifecycleObject,
}

pub(crate) fn action_commit_object(
  state: &mut State,
  txn: &mut Transaction,
  ActionCommitObjectInput { obj }: ActionCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let now = get_now_ms();
  let key = TinyBuf::from_slice(obj.key());
  let object_id = obj.id();

  txn.record(
    obj.dev_offset(),
    obj.build_raw_data_with_new_object_state(&state.pages, ObjectState::Committed),
  );

  let existing = state.committed_objects.insert(key.clone(), obj);
  if let Some(existing) = existing {
    action_delete_object(state, txn, ActionDeleteObjectInput { obj: existing }).unwrap();
  };

  state
    .stream
    .write()
    .add_event_pending_commit(StreamEventOwned {
      typ: StreamEventType::ObjectCommit,
      timestamp_ms: now,
      object_id,
      key,
    });

  Ok(OpCommitObjectOutput { object_id })
}
