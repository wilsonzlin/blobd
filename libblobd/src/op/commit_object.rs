use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_slots::IncompleteSlotId;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::op::key_debug_str;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use off64::create_u48_be;
use std::sync::Arc;
use tracing::trace;
use write_journal::AtomicWriteGroup;

pub struct OpCommitObjectInput {
  pub key: Vec<u8>,
  pub object_id: u64,
  pub incomplete_slot_id: IncompleteSlotId,
}

pub struct OpCommitObjectOutput {}

// See op_delete_object for why we hold RwLock write lock for entire request.
pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let mut writes = Vec::with_capacity(4);

  let Some(incomplete_slot_lock) = ctx.incomplete_slots.lock_slot_for_committing_then_vacate(&mut writes, req.incomplete_slot_id, req.object_id).await else {
    return Err(OpError::ObjectNotFound);
  };

  let bkt_id = ctx.buckets.bucket_id_for_key(&req.key);
  let mut bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let inode_dev_offset = incomplete_slot_lock.inode_dev_offset();
  trace!(
    key = key_debug_str(&req.key),
    object_id = req.object_id,
    inode_dev_offset,
    "committing object"
  );

  // We have to acquire a change serial even though we don't change the free list.
  let change_serial = ctx.free_list.lock().await.generate_change_serial();
  bkt.version += 1;

  // Update bucket head to point to this new inode.
  let cur_bkt_head = ctx.buckets.get_bucket_head(bkt_id).await;
  ctx
    .buckets
    .mutate_bucket_head(&mut writes, bkt_id, inode_dev_offset);

  // Update inode next pointer.
  writes.push((
    inode_dev_offset + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET,
    create_u48_be(cur_bkt_head).to_vec(),
  ));

  // Create stream event.
  ctx
    .stream
    .write()
    .await
    .create_event(&mut writes, StreamEvent {
      typ: StreamEventType::ObjectCommit,
      bucket_id: bkt_id,
      object_id: req.object_id,
    });

  ctx
    .journal
    .write(change_serial, AtomicWriteGroup(writes))
    .await;

  trace!(
    key = key_debug_str(&req.key),
    object_id = req.object_id,
    inode_dev_offset,
    "committed object"
  );

  Ok(OpCommitObjectOutput {})
}