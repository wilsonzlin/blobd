use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::state::action::commit_object::ActionCommitObjectInput;
use crate::state::StateAction;
use signal_future::SignalFuture;
use std::sync::Arc;

pub struct OpCommitObjectInput {
  pub incomplete_token: IncompleteToken,
}

pub struct OpCommitObjectOutput {
  pub object_id: u64,
}

pub(crate) async fn op_commit_object(
  ctx: Arc<Ctx>,
  req: OpCommitObjectInput,
) -> OpResult<OpCommitObjectOutput> {
  let Some(bkt) = ctx.buckets.get_bucket_by_id(req.incomplete_token.bucket_id) else {
    return Err(OpError::ObjectNotFound);
  };

  let Some(key) = bkt.find_object_with_id_and_read_key(ctx.device.clone(), req.incomplete_token.object_id).await else {
    return Err(OpError::ObjectNotFound);
  };

  // SAFETY: If we find ourselves (because someone else has committed our incomplete token), we'll fail later.
  let existing = if ctx.versioning {
    None
  } else {
    bkt
      .find_object(ctx.device.clone(), &key, None)
      .await
      .map(|o| o.id)
  };

  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Commit(
    ActionCommitObjectInput {
      also_delete_object_in_same_bucket_with_id: existing,
      incomplete_token: req.incomplete_token,
      key: key.into(),
    },
    fut_ctl,
  ));

  fut.await
}
