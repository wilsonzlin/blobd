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
use libblobd_lite::op::create_object::OpCreateObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use tinybuf::TinyBuf;

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
    key: key.to_vec(),
    size: req.size,
  }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let res = ctx
    .blobd
    .create_object(OpCreateObjectInput {
      key,
      size: req.size,
      assoc_data: TinyBuf::empty(),
    })
    .await;

  match res {
    Ok(c) => {
      let upload_token = ctx.generate_upload_token(c.token, req.size);

      let mut headers = HeaderMap::new();
      headers.insert(
        "x-blobd-upload-token",
        HeaderValue::from_str(&upload_token).unwrap(),
      );
      (StatusCode::ACCEPTED, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
