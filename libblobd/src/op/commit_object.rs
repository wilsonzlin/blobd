use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::inode::InodeState;
use crate::inode::INO_OFFSETOF_KEY_LEN;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_OBJ_ID;
use crate::inode::INO_OFFSETOF_STATE;
use crate::op::key_debug_str;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use off64::create_u48_be;
use off64::usz;
use off64::Off64Int;
use std::sync::Arc;
use tracing::trace;
use write_journal::AtomicWriteGroup;

pub struct OpCommitObjectInput {
  pub key: Vec<u8>,
  pub object_id: u64,
  pub inode_dev_offset: u64,
}

pub struct OpCommitObjectOutput {}

// See op_delete_object for why we hold RwLock write lock for entire request.
pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let bkt_id = ctx.buckets.bucket_id_for_key(&req.key);
  let mut bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let inode_dev_offset = req.inode_dev_offset;
  trace!(
    key = key_debug_str(&req.key),
    object_id = req.object_id,
    inode_dev_offset,
    "committing object"
  );

  // Check AFTER acquiring lock in case two requests try to commit the same inode.
  {
    let base = INO_OFFSETOF_STATE;
    let raw = ctx
      .device
      .read_at(inode_dev_offset + base, INO_OFFSETOF_KEY_LEN - base)
      .await;
    let state = raw[usz!(INO_OFFSETOF_STATE - base)];
    let obj_id = raw.read_u64_be_at(INO_OFFSETOF_OBJ_ID - base);
    if state != InodeState::Incomplete as u8 || obj_id != req.object_id {
      return Err(OpError::ObjectNotFound);
    };
  };

  // We have to acquire a change serial even though we don't change the free list.
  let change_serial = ctx.free_list.lock().await.generate_change_serial();
  bkt.version += 1;

  let mut writes = Vec::with_capacity(4);

  // Update bucket head to point to this new inode.
  let cur_bkt_head = ctx.buckets.get_bucket_head(bkt_id).await;
  ctx
    .buckets
    .mutate_bucket_head(&mut writes, bkt_id, inode_dev_offset);

  // Update inode state.
  writes.push((inode_dev_offset + INO_OFFSETOF_STATE, vec![
    InodeState::Ready as u8,
  ]));

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
    .lock()
    .await
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
