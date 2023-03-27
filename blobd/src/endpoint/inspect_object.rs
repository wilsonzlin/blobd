use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header::CONTENT_LENGTH;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use libblobd::op::inspect_object::OpInspectObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub t: String,
}

pub async fn endpoint_inspect_object(
  State(ctx): State<Arc<HttpCtx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> (StatusCode, HeaderMap) {
  let key = parse_key(&uri);
  if AuthToken::verify(&ctx.tokens, &req.t, AuthTokenAction::InspectObject {
    key: key.clone(),
  }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let res = ctx.blobd.inspect_object(OpInspectObjectInput { key }).await;

  match res {
    Ok(o) => {
      let mut headers = HeaderMap::new();
      headers.insert(CONTENT_LENGTH, o.size.into());
      headers.insert("x-blobd-object-id", o.object_id.into());
      (StatusCode::OK, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
