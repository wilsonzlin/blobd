use super::OpError;
use super::OpResult;
use crate::allocator::Allocations;
use crate::ctx::Ctx;
use crate::object::ObjectState;
use std::sync::Arc;

pub struct OpDeleteObjectInput {
  pub key: Vec<u8>,
  // Only useful if versioning is enabled.
  pub id: Option<u128>,
}

pub struct OpDeleteObjectOutput {}

// We hold write lock on bucket RwLock for entire request (including writes and data sync) for simplicity and avoidance of subtle race conditions. Performance should still be great as one bucket equals one object given desired bucket count and load. If we release lock before we (or journal) finishes writes, we need to prevent/handle any possible intermediate read and write of the state of inode elements on the device, linked list pointers, garbage collectors, premature use of data or reuse of freed space, etc.
pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let (txn, to_free, overlay_entry) = {
    let mut bkt = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
    let mut txn = bkt.begin_transaction();
    let mut to_free = Allocations::new();

    let obj = bkt.find_object(ObjectState::Committed, req.id).await;
    let overlay_entry = match obj {
      Some(obj) => Some(bkt.delete_object(&mut txn, &mut to_free, obj).await),
      None => None,
    };

    (txn, to_free, overlay_entry)
  };

  // We must always commit the transaction (otherwise our journal will wait forever), so we cannot return before this if the object does not exist.
  ctx.buckets.commit_transaction(txn).await;

  let Some(overlay_entry) = overlay_entry else {
    return Err(OpError::ObjectNotFound);
  };

  // We must release the allocations AFTER ensuring on-disk state has persisted, or else we may race ahead and allocate those freed pages from objects while they still exist on disk (not yet deleted).
  ctx.allocator.lock().release_all(&to_free);
  ctx.overlay.evict(overlay_entry);

  Ok(OpDeleteObjectOutput {})
}
