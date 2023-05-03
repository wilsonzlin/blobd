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

pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let Some((_, obj)) = ctx.committed_objects.remove_if(&req.key, |_, o| req.id.is_none() || Some(o.id()) == req.id) else {
    return Err(OpError::ObjectNotFound);
  };

  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Delete(
    ActionDeleteObjectInput { obj },
    fut_ctl,
  ));

  fut.await
}
