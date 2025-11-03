use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use itertools::Itertools;
use libblobd_direct::op::commit_object::OpCommitObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
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
  if !ctx.verify_auth(&req.t, AuthTokenAction::CommitObject { key: key.to_vec() }) {
    return StatusCode::UNAUTHORIZED;
  };

  let Some((incomplete_token, object_size)) = ctx.parse_and_verify_upload_token(&req.upload_token)
  else {
    return StatusCode::NOT_FOUND;
  };

  let write_receipts = req.write_receipts.split(",").collect_vec();
  if ctx.verify_write_receipts(&write_receipts, incomplete_token, object_size) == None {
    return StatusCode::PRECONDITION_FAILED;
  };

  let res = ctx
    .blobd
    .commit_object(OpCommitObjectInput {
      incomplete_token,
      if_not_exists: false,
    })
    .await;

  match res {
    Ok(_) => StatusCode::CREATED,
    Err(err) => transform_op_error(err),
  }
}
