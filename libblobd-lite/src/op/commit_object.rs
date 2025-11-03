use super::OpError;
use super::OpResult;
use crate::allocator::Allocations;
use crate::bucket::FoundObject;
use crate::ctx::Ctx;
use crate::object::OBJECT_OFF;
use crate::object::ObjectState;
use crate::op::key_debug_str;
use futures::StreamExt;
use futures::pin_mut;
use std::sync::Arc;
use tracing::instrument;
use tracing::trace;

pub struct OpCommitObjectInput {
  pub key: Vec<u8>,
  pub object_id: u128,
}

pub struct OpCommitObjectOutput {}

#[instrument(skip_all)]
pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let object_id = req.object_id;

  let mut to_free = Allocations::new();
  let (txn, deletion_overlay_entry, overlay_entry) = {
    let locker = ctx.buckets.get_locker_for_key(&req.key);
    let mut bkt = locker.write().await;
    let (
      ex,
      FoundObject {
        dev_offset: object_dev_offset,
        ..
      },
    ) = {
      let mut incomplete_object = None;
      let mut existing_object = None;
      let iter = bkt.iter();
      pin_mut!(iter);
      while let Some(o) = iter.next().await {
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

    let mut txn = bkt.begin_transaction();

    let deletion_overlay_entry = match ex {
      Some(ex) if !ctx.versioning => Some(bkt.delete_object(&mut txn, &mut to_free, ex).await),
      _ => None,
    };

    // Detach from in-memory incomplete list.
    // TODO

    // Update inode state.
    let overlay_entry = ctx
      .overlay
      .set_object_state(object_id, ObjectState::Committed);
    txn.write(object_dev_offset + OBJECT_OFF.state(), vec![
      ObjectState::Committed as u8,
    ]);

    (
      ctx.buckets.commit_transaction(txn),
      deletion_overlay_entry,
      overlay_entry,
    )
  };
  if let Some(signal) = txn {
    signal.await;
  }
  ctx.allocator.lock().release_all(&to_free);
  ctx.overlay.evict(overlay_entry);
  if let Some(deletion_overlay_entry) = deletion_overlay_entry {
    ctx.overlay.evict(deletion_overlay_entry);
  }

  trace!(key = key_debug_str(&req.key), object_id, "committed object");

  Ok(OpCommitObjectOutput {})
}
