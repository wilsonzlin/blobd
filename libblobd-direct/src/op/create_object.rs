use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::state::action::create_object::ActionCreateObjectInput;
use crate::state::StateAction;
use signal_future::SignalFuture;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpCreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
  pub assoc_data: TinyBuf,
}

pub struct OpCreateObjectOutput {
  pub token: IncompleteToken,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Create(
    ActionCreateObjectInput {
      assoc_data: req.assoc_data,
      key: req.key,
      size: req.size,
    },
    fut_ctl,
  ));
  fut.await
}
