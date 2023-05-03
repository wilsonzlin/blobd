use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::OBJECT_OFF;
use crate::op::key_debug_str;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use off64::int::Off64AsyncReadInt;
use std::sync::Arc;
use tracing::trace;

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
  let object_dev_offset = req.incomplete_token.object_dev_offset;

  // See IncompleteToken for why if the token has not expired, the object definitely still exists (i.e. safe to read any metadata).
  if req
    .incomplete_token
    .has_expired(ctx.reap_objects_after_secs)
  {
    return Err(OpError::ObjectNotFound);
  };

  let object_id = ctx
    .device
    .read_u64_be_at(object_dev_offset + OBJECT_OFF.id())
    .await;
  let key_len = ctx
    .device
    .read_u16_be_at(object_dev_offset + OBJECT_OFF.key_len())
    .await;
  let key = ctx
    .device
    .read_at(object_dev_offset + OBJECT_OFF.key(), key_len.into())
    .await;

  let (txn, event) = {
    let mut state = ctx.state.lock().await;

    let mut bkt = ctx.buckets.get_bucket_mut_for_key(&key).await;
    trace!(
      key = key_debug_str(&key),
      object_id,
      object_dev_offset,
      "committing object"
    );

    // Check while holding lock to prevent two commits to the same object.
    let hdr = ctx
      .pages
      .read_page_header::<ObjectPageHeader>(object_dev_offset)
      .await;
    if hdr.state != ObjectState::Incomplete {
      return Err(OpError::ObjectNotFound);
    };

    // Don't begin transaction until after possible previous `return` (otherwise our journal will wait forever for the transaction to commit).
    let mut txn = ctx.journal.begin_transaction();

    if !ctx.versioning {
      // This will create an event for any deletion, which we want (we don't just want a commit event, as then anyone reading the stream must tracked all seen keys to know when a commit deletes an existing object).
      bkt
        .move_object_to_deleted_list_if_exists(&mut txn, &mut state, None)
        .await;
    };

    // Detach from incomplete list.
    state
      .incomplete_list
      .detach(&mut txn, object_dev_offset)
      .await;

    // Get the current bucket head. We use the overlay, so we'll see any change made by the previous `move_object_to_deleted_list_if_exists` call.
    let cur_bkt_head = bkt.get_head().await;

    // Update bucket head to point to this new inode.
    bkt.mutate_head(&mut txn, object_dev_offset);

    // Update inode next pointer.
    ctx
      .pages
      .update_page_header::<ObjectPageHeader>(&mut txn, object_dev_offset, |o| {
        debug_assert_eq!(o.state, ObjectState::Incomplete);
        debug_assert_eq!(o.deleted_sec, None);
        o.state = ObjectState::Committed;
        o.next = cur_bkt_head;
      })
      .await;

    // Create stream event.
    let event = state.stream.create_event_on_device(&mut txn, StreamEvent {
      typ: StreamEventType::ObjectCommit,
      bucket_id: bkt.bucket_id(),
      object_id,
    });

    (txn, event)
  };

  ctx.journal.commit_transaction(txn).await;

  ctx.stream_in_memory.add_event_to_in_memory_list(event);

  trace!(
    key = key_debug_str(&key),
    object_id,
    object_dev_offset,
    "committed object"
  );

  Ok(OpCommitObjectOutput { object_id })
}
