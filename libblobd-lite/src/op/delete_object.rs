use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpDeleteObjectInput {
  pub key: TinyBuf,
  // Only useful if versioning is enabled.
  pub id: Option<u64>,
}

pub struct OpDeleteObjectOutput {}

// We hold write lock on bucket RwLock for entire request (including writes and data sync) for simplicity and avoidance of subtle race conditions. Performance should still be great as one bucket equals one object given desired bucket count and load. If we release lock before we (or journal) finishes writes, we need to prevent/handle any possible intermediate read and write of the state of inode elements on the device, linked list pointers, garbage collectors, premature use of data or reuse of freed space, etc.
pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let (txn, deleted) = {
    let mut state = ctx.state.lock().await;
    let mut txn = ctx.journal.begin_transaction();

    let mut bkt = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
    // We must always commit the transaction (otherwise our journal will wait forever), so we cannot return directly here if the object does not exist.
    let deleted = bkt
      .move_object_to_deleted_list_if_exists(&mut txn, &mut state, req.id)
      .await;

    (txn, deleted)
  };

  // We must always commit the transaction (otherwise our journal will wait forever), so we cannot return before this if the object does not exist.
  ctx.journal.commit_transaction(txn).await;

  let Some(e) = deleted else {
    return Err(OpError::ObjectNotFound);
  };
  ctx.stream_in_memory.add_event_to_in_memory_list(e);

  Ok(OpDeleteObjectOutput {})
}
