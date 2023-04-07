use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthTokenAction;
use libblobd::op::create_object::OpCreateObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub size: u64,
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_create_object(
  State(ctx): State<Arc<HttpCtx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> (StatusCode, HeaderMap) {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::CreateObject {
    key: key.clone(),
    size: req.size,
  }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let res = ctx
    .blobd
    .create_object(OpCreateObjectInput {
      key,
      size: req.size,
    })
    .await;

  match res {
    Ok(c) => {
      let upload_token = ctx.generate_upload_token(c.object_id, c.incomplete_slot_id, req.size);

      let mut headers = HeaderMap::new();
      headers.insert("x-blobd-object-id", c.object_id.into());
      headers.insert(
        "x-blobd-upload-token",
        HeaderValue::from_str(&upload_token).unwrap(),
      );
      (StatusCode::ACCEPTED, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
