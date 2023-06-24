use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use crate::libblobd::op::inspect_object::OpInspectObjectInput;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header::CONTENT_LENGTH;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_inspect_object(
  State(ctx): State<Arc<HttpCtx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> (StatusCode, HeaderMap) {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::InspectObject { key: key.to_vec() }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let res = ctx
    .blobd
    .inspect_object(OpInspectObjectInput { key, id: None })
    .await;

  match res {
    Ok(o) => {
      let mut headers = HeaderMap::new();
      headers.insert(CONTENT_LENGTH, o.size.into());
      headers.insert("x-blobd-object-id", o.id.into());
      (StatusCode::OK, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
