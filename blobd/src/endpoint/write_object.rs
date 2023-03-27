use super::transform_op_error;
use super::HttpCtx;
use super::UploadId;
use axum::extract::BodyStream;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::ContentLength;
use axum::http::StatusCode;
use axum::TypedHeader;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use futures::StreamExt;
use libblobd::op::write_object::OpWriteObjectInput;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub offset: u64,
  pub object_id: u64,
  pub upload_id: String,
  pub t: String,
}

pub async fn endpoint_write_object(
  State(ctx): State<Arc<HttpCtx>>,
  TypedHeader(ContentLength(len)): TypedHeader<ContentLength>,
  req: Query<InputQueryParams>,
  body: BodyStream,
) -> StatusCode {
  if AuthToken::verify(&ctx.tokens, &req.t, AuthTokenAction::WriteObject {
    object_id: req.object_id,
    offset: req.offset,
  }) {
    return StatusCode::UNAUTHORIZED;
  };

  let Some(inode_dev_offset) = UploadId::parse_and_verify(&ctx.tokens, &req.upload_id) else {
    return StatusCode::NOT_FOUND;
  };

  let res = ctx
    .blobd
    .write_object(OpWriteObjectInput {
      data_len: len,
      inode_dev_offset,
      object_id: req.object_id,
      offset: req.offset,
      data_stream: body.map(|chunk| {
        chunk
          .map(|bytes| bytes.to_vec())
          .map_err(|err| Box::from(err))
      }),
    })
    .await;

  match res {
    Ok(_) => StatusCode::ACCEPTED,
    Err(err) => transform_op_error(err),
  }
}
