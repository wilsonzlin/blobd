use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use itertools::Itertools;
use libblobd::op::commit_object::OpCommitObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub object_id: u64,
  pub upload_token: String,
  // Comma separated, in order.
  pub write_receipts: String,
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_commit_object(
  State(ctx): State<Arc<HttpCtx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> StatusCode {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::CommitObject {
    object_id: req.object_id,
  }) {
    return StatusCode::UNAUTHORIZED;
  };

  let Some((incomplete_slot_id, part_count)) = ctx.parse_and_verify_upload_token(req.object_id, &req.upload_token) else {
    return StatusCode::NOT_FOUND;
  };

  let write_receipts = req.write_receipts.split(",").collect_vec();
  if !ctx.verify_write_receipts(
    &write_receipts,
    req.object_id,
    incomplete_slot_id,
    part_count,
  ) {
    return StatusCode::PRECONDITION_FAILED;
  };

  let res = ctx
    .blobd
    .commit_object(OpCommitObjectInput {
      incomplete_slot_id,
      key,
      object_id: req.object_id,
    })
    .await;

  match res {
    Ok(_) => StatusCode::CREATED,
    Err(err) => transform_op_error(err),
  }
}
