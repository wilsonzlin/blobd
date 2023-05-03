use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::ObjectState;
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
  let Some(obj) = ctx.incomplete_objects.write().remove(&req.incomplete_token.object_id) else {
    return Err(OpError::ObjectNotFound);
  };

  obj
    .update_state_then_ensure_no_writers(ObjectState::Committed)
    .await;

  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Commit(
    ActionCommitObjectInput { obj },
    fut_ctl,
  ));

  fut.await
}
