use super::transform_op_error;
use super::HttpCtx;
use axum::extract::BodyStream;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::ContentLength;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::TypedHeader;
use blobd_token::AuthTokenAction;
use futures::StreamExt;
use libblobd::op::write_object::OpWriteObjectInput;
use libblobd::tile::TILE_SIZE_U64;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub offset: u64,
  pub object_id: u64,
  pub upload_token: String,
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_write_object(
  State(ctx): State<Arc<HttpCtx>>,
  TypedHeader(ContentLength(len)): TypedHeader<ContentLength>,
  req: Query<InputQueryParams>,
  body: BodyStream,
) -> (StatusCode, HeaderMap) {
  if !ctx.verify_auth(&req.t, AuthTokenAction::WriteObject {
    object_id: req.object_id,
  }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let Some((incomplete_slot_id, _)) = ctx.parse_and_verify_upload_token(req.object_id, &req.upload_token) else {
    return (StatusCode::NOT_FOUND,  HeaderMap::new());
  };

  let res = ctx
    .blobd
    .write_object(OpWriteObjectInput {
      data_len: len,
      incomplete_slot_id,
      object_id: req.object_id,
      offset: req.offset,
      data_stream: body.map(|chunk| {
        chunk
          .map(|bytes| bytes.to_vec())
          .map_err(|err| Box::from(err))
      }),
    })
    .await;

  let receipt = ctx.generate_write_receipt(
    req.object_id,
    incomplete_slot_id,
    req.offset / TILE_SIZE_U64,
  );

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
