use super::OpError;
use super::OpResult;
use crate::allocator::Allocations;
use crate::ctx::Ctx;
use crate::object::ObjectState;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

pub struct OpDeleteObjectInput {
  pub key: Vec<u8>,
  // Only useful if versioning is enabled.
  pub id: Option<u64>,
}

pub struct OpDeleteObjectOutput {}

// We hold write lock on bucket RwLock for entire request (including writes and data sync) for simplicity and avoidance of subtle race conditions. Performance should still be great as one bucket equals one object given desired bucket count and load. If we release lock before we (or journal) finishes writes, we need to prevent/handle any possible intermediate read and write of the state of inode elements on the device, linked list pointers, garbage collectors, premature use of data or reuse of freed space, etc.
pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let (txn, to_free, meta) = {
    let mut bkt = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
    let mut txn = bkt.begin_transaction();
    let mut to_free = Allocations::new();

    let obj = bkt.find_object(ObjectState::Committed, req.id).await;
    let meta = if let Some(obj) = obj {
      let size = obj.size();
      let metadata_size = obj.metadata_size();
      bkt.delete_object(&mut txn, &mut to_free, obj).await;
      Some((size, metadata_size))
    } else {
      None
    };

    (txn, to_free, meta)
  };

  // We must always commit the transaction (otherwise our journal will wait forever), so we cannot return before this if the object does not exist.
  ctx.buckets.commit_transaction(txn).await;

  let Some((size, metadata_size)) = meta else {
    return Err(OpError::ObjectNotFound);
  };

  // We must release the allocations AFTER ensuring on-disk state has persisted, or else we may race ahead and allocate those freed pages from objects while they still exist on disk (not yet deleted).
  ctx.allocator.lock().release_all(&to_free);

  ctx.metrics.deleted_object_count.fetch_sub(1, Relaxed);
  ctx.metrics.object_count.fetch_sub(1, Relaxed);
  ctx.metrics.object_data_bytes.fetch_sub(size, Relaxed);
  ctx
    .metrics
    .object_metadata_bytes
    .fetch_sub(metadata_size, Relaxed);

  Ok(OpDeleteObjectOutput {})
}
