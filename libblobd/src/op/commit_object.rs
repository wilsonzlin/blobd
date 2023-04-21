use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::op::key_debug_str;
use crate::page::ActiveInodePageHeader;
use crate::page::IncompleteInodePageHeader;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use off64::u16;
use std::sync::Arc;
use tracing::trace;

pub struct OpCommitObjectInput {
  pub key: Vec<u8>,
  pub object_id: u64,
  pub inode_dev_offset: u64,
}

pub struct OpCommitObjectOutput {}

// TODO WARNING: Ensure there are no current or future write calls to the object before calling this function.
// TODO WARNING: Ensure that the object exists, is currently in an incomplete state, and has not expired.
pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let key_len = u16!(req.key.len());
  let inode_dev_offset = req.inode_dev_offset;

  let Some(hdr) = ctx.pages.read_page_header::<IncompleteInodePageHeader>(req.inode_dev_offset).await else {
    return Err(OpError::ObjectNotFound);
  };

  if hdr.has_expired(ctx.incomplete_objects_expire_after_hours) {
    return Err(OpError::ObjectNotFound);
  };

  let txn = {
    let mut state = ctx.state.lock().await;
    let mut txn = ctx.journal.begin_transaction();

    let mut bkt_lock = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
    trace!(
      key = key_debug_str(&req.key),
      object_id = req.object_id,
      inode_dev_offset,
      "committing object"
    );

    // This will create an event for any deletion, which we want (we don't just want a commit event, as then anyone reading the stream must tracked all seen keys to know when a commit deletes an existing object).
    bkt_lock
      .move_object_to_deleted_list_if_exists(&mut txn, &mut state.stream)
      .await;

    // Get the current bucket head. We use the overlay, so we'll see any change made by the previous `move_object_to_deleted_list_if_exists` call.
    let cur_bkt_head = bkt_lock.get_head().await;

    // Update bucket head to point to this new inode.
    bkt_lock.mutate_head(&mut txn, inode_dev_offset);

    // Update inode next pointer.
    ctx
      .pages
      .write_page_header(&mut txn, inode_dev_offset, ActiveInodePageHeader {
        next: cur_bkt_head,
      });

    // Create stream event.
    state.stream.create_event(&mut txn, StreamEvent {
      typ: StreamEventType::ObjectCommit,
      bucket_id: bkt_lock.bucket_id(),
      object_id: req.object_id,
    });

    txn
  };

  ctx.journal.commit_transaction(txn).await;

  trace!(
    key = key_debug_str(&req.key),
    object_id = req.object_id,
    inode_dev_offset,
    "committed object"
  );

  Ok(OpCommitObjectOutput {})
}
