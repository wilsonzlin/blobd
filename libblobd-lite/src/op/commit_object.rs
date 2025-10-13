use super::OpError;
use super::OpResult;
use crate::allocator::Allocations;
use crate::bucket::FoundObject;
use crate::ctx::Ctx;
use crate::object::OBJECT_OFF;
use crate::object::ObjectState;
use crate::op::key_debug_str;
use futures::StreamExt;
use std::sync::Arc;
use tracing::trace;

pub struct OpCommitObjectInput {
  pub key: Vec<u8>,
  pub object_id: u64,
}

pub struct OpCommitObjectOutput {}

pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let object_id = req.object_id;

  let bkt = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
  let (
    ex,
    FoundObject {
      dev_offset: object_dev_offset,
      ..
    },
  ) = {
    let mut incomplete_object = None;
    let mut existing_object = None;
    while let Some(o) = bkt.iter().next().await {
      if o.state() == ObjectState::Incomplete && o.id() == object_id {
        incomplete_object = Some(o);
      } else if o.state() == ObjectState::Committed {
        existing_object = Some(o);
      }
    }
    let Some(incomplete_object) = incomplete_object else {
      return Err(OpError::ObjectNotFound);
    };
    (existing_object, incomplete_object)
  };
  trace!(
    key = key_debug_str(&req.key),
    object_id, object_dev_offset, "committing object"
  );

  let mut to_free = Allocations::new();
  let txn = {
    let mut bkt = ctx.buckets.get_bucket_mut_for_key(&req.key).await;
    let mut txn = bkt.begin_transaction();

    if !ctx.versioning
      && let Some(ex) = ex
    {
      bkt.delete_object(&mut txn, &mut to_free, ex).await;
    };

    // Detach from in-memory incomplete list.
    // TODO

    // Update inode state.
    ctx
      .overlay
      .set_object_state(object_id, ObjectState::Committed);
    txn.write(object_dev_offset + OBJECT_OFF.state(), vec![
      ObjectState::Committed as u8,
    ]);

    txn
  };
  ctx.buckets.commit_transaction(txn).await;

  trace!(
    key = key_debug_str(&req.key),
    object_id, object_dev_offset, "committed object"
  );

  Ok(OpCommitObjectOutput {})
}
