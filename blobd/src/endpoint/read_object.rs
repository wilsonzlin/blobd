use super::parse_key;
use super::transform_op_error;
use super::HttpCtx;
use axum::body::StreamBody;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::Range;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::Response;
use axum::TypedHeader;
use blobd_token::AuthTokenAction;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use libblobd_direct::op::read_object::OpReadObjectInput;
use libblobd_direct::op::read_object::OpReadObjectOutput;
use serde::Deserialize;
use serde::Serialize;
use std::io;
use std::ops::Bound;
use std::sync::Arc;

const STREAM_BUFSIZE: u64 = 1024 * 8;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  #[serde(default)]
  pub t: String,
}

pub async fn endpoint_read_object(
  State(ctx): State<Arc<HttpCtx>>,
  ranges: Option<TypedHeader<Range>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> Result<Response<StreamBody<impl Stream<Item = Result<Bytes, io::Error>>>>, StatusCode> {
  let key = parse_key(&uri);
  if !ctx.verify_auth(&req.t, AuthTokenAction::ReadObject { key: key.to_vec() }) {
    return Err(StatusCode::UNAUTHORIZED);
  };

  let ranges = ranges
    .map(|ranges| ranges.iter().collect_vec())
    .unwrap_or_default();
  if ranges.len() > 1 {
    // We currently don't support multirange requests.
    return Err(StatusCode::RANGE_NOT_SATISFIABLE);
  };
  let range = ranges
    .first()
    .cloned()
    .unwrap_or((Bound::Unbounded, Bound::Unbounded));

  let start = match range.0 {
    Bound::Included(v) => v,
    // Lower bound must always be inclusive.
    Bound::Excluded(_) => return Err(StatusCode::RANGE_NOT_SATISFIABLE),
    Bound::Unbounded => 0,
  };
  // Exclusive.
  let end = match range.1 {
    Bound::Included(v) => Some(v + 1),
    Bound::Excluded(v) => Some(v),
    Bound::Unbounded => None,
  };

  let res = ctx
    .blobd
    .read_object(OpReadObjectInput {
      end,
      key,
      start,
      stream_buffer_size: STREAM_BUFSIZE,
      id: None,
    })
    .await;

  match res {
    Ok(OpReadObjectOutput {
      data_stream,
      end,
      object_id,
      object_size,
      start,
    }) => Ok(
      Response::builder()
        .status(if ranges.is_empty() {
          StatusCode::OK
        } else {
          StatusCode::PARTIAL_CONTENT
        })
        .header("accept-ranges", "bytes")
        .header("content-length", (end - start).to_string())
        .header(
          "content-range",
          format!("bytes {start}-{}/{object_size}", end.saturating_sub(1)),
        )
        .header("x-blobd-object-id", object_id.to_string())
        .body(StreamBody::new(data_stream.map(|chunk| {
          chunk
            .map(|chunk| Bytes::from(chunk.to_vec()))
            // The only error type is OpError::ObjectNotFound.
            .map_err(|err| io::Error::new(io::ErrorKind::NotFound, err))
        })))
        .unwrap(),
    ),
    Err(err) => Err(transform_op_error(err)),
  }
}
