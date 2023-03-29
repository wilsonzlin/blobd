use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use super::UploadId;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
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
      let mut headers = HeaderMap::new();
      headers.insert("x-blobd-object-id", c.object_id.into());
      headers.insert(
        "x-blobd-upload-id",
        UploadId::new(&ctx.tokens, c.inode_dev_offset)
          .parse()
          .unwrap(),
      );
      (StatusCode::ACCEPTED, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
