use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use super::UploadId;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use libblobd::op::commit_object::OpCommitObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub object_id: u64,
  pub upload_id: String,
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

  let Some(inode_dev_offset) = UploadId::parse_and_verify(&ctx.tokens, &req.upload_id) else {
    return StatusCode::NOT_FOUND;
  };

  let res = ctx
    .blobd
    .commit_object(OpCommitObjectInput {
      inode_dev_offset,
      key,
      object_id: req.object_id,
    })
    .await;

  match res {
    Ok(_) => StatusCode::CREATED,
    Err(err) => transform_op_error(err),
  }
}
