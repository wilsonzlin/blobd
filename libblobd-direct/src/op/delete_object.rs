use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::state::action::delete_object::ActionDeleteObjectInput;
use crate::state::StateAction;
use signal_future::SignalFuture;
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
  let bucket_id = ctx.buckets.bucket_id_for_key(&req.key);
  let Some(bkt) = ctx.buckets.get_bucket_by_id(bucket_id) else {
    return Err(OpError::ObjectNotFound);
  };

  let Some(obj) = bkt.find_object(ctx.device.clone(), &req.key, req.id).await else {
    return Err(OpError::ObjectNotFound);
  };

  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Delete(
    ActionDeleteObjectInput {
      bucket_id,
      object_id: obj.id,
      key: req.key,
    },
    fut_ctl,
  ));

  fut.await
}
