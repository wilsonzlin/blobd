use super::OpError;
use super::OpResult;
use crate::bucket::Bucket;
use crate::bucket::FoundInode;
use crate::ctx::ChangeSerial;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILES;
use crate::inode::INO_SIZE;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use itertools::Itertools;
use off64::create_u48_be;
use off64::Off64Int;
use std::sync::Arc;
use tokio::sync::RwLockWriteGuard;
use write_journal::AtomicWriteGroup;

pub struct OpDeleteObjectInput {
  pub key: Vec<u8>,
}

pub struct OpDeleteObjectOutput {}

// We hold write lock on bucket RwLock for entire request (including writes and data sync) for simplicity and avoidance of subtle race conditions. Performance should still be great as one bucket equals one object given desired bucket count and load. If we release lock before we (or journal) finishes writes, we need to prevent/handle any possible intermediate read and write of the state of inode elements on the device, linked list pointers, garbage collectors, premature use of data or reuse of freed space, etc.
pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();

  let mut writes = Vec::new();

  let bkt_id = ctx.buckets.bucket_id_for_key(&req.key);
  let mut bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let Some(del) = delete_object_in_locked_bucket_if_exists(&mut writes, &ctx, &mut bkt, bkt_id, &req.key, key_len).await else {
    return Err(OpError::ObjectNotFound);
  };

  if let Some(new_bucket_head) = del.new_bucket_head {
    // Update bucket head.
    ctx
      .buckets
      .mutate_bucket_head(&mut writes, bkt_id, new_bucket_head);
  };

  // Create stream event.
  ctx
    .stream
    .write()
    .await
    .create_event(&mut writes, StreamEvent {
      typ: StreamEventType::ObjectDelete,
      bucket_id: bkt_id,
      object_id: del.object_id,
    });

  ctx
    .journal
    .write(del.change_serial, AtomicWriteGroup(writes))
    .await;

  Ok(OpDeleteObjectOutput {})
}
