use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use crate::libblobd::op::write_object::OpWriteObjectInput;
use axum::extract::BodyStream;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::ContentLength;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::TypedHeader;
use blobd_token::AuthTokenAction;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub offset: u64,
  pub upload_token: String,
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_write_object(
  State(ctx): State<Arc<HttpCtx>>,
  TypedHeader(ContentLength(len)): TypedHeader<ContentLength>,
  uri: Uri,
  req: Query<InputQueryParams>,
  body: BodyStream,
) -> (StatusCode, HeaderMap) {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::WriteObject { key: key.to_vec() }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let Some((incomplete_token, _)) = ctx.parse_and_verify_upload_token(&req.upload_token) else {
    return (StatusCode::NOT_FOUND, HeaderMap::new());
  };

  let res = ctx
    .blobd
    .write_object(OpWriteObjectInput {
      data_len: len,
      incomplete_token,
      offset: req.offset,
      data_stream: body.map(|chunk| {
        chunk
          .map(|bytes| bytes.to_vec())
          .map_err(|err| Box::from(err))
      }),
    })
    .await;

  let receipt =
    ctx.generate_write_receipt(incomplete_token, req.offset / ctx.blobd.cfg().lpage_size());

  match res {
    Ok(_) => {
      let mut headers = HeaderMap::new();
      headers.insert(
        "x-blobd-write-receipt",
        HeaderValue::from_str(&receipt).unwrap(),
      );
      (StatusCode::ACCEPTED, headers)
    }
    Err(err) => (transform_op_error(err), HeaderMap::new()),
  }
}
