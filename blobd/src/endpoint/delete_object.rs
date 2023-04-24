use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use libblobd::op::delete_object::OpDeleteObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_delete_object(
  State(ctx): State<Arc<HttpCtx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> StatusCode {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::DeleteObject { key: key.to_vec() }) {
    return StatusCode::UNAUTHORIZED;
  };

  let res = ctx
    .blobd
    .delete_object(OpDeleteObjectInput { key, id: None })
    .await;

  match res {
    Ok(_) => StatusCode::OK,
    Err(err) => transform_op_error(err),
  }
}
