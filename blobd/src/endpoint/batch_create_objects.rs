use super::HttpCtx;
use crate::libblobd::op::commit_object::OpCommitObjectInput;
use crate::libblobd::op::create_object::OpCreateObjectInput;
use crate::libblobd::op::write_object::OpWriteObjectInput;
use axum::extract::BodyStream;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use blobd_token::AuthTokenAction;
use futures::stream::once;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::TryStreamExt;
use off64::int::Off64ReadInt;
use off64::usz;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::min;
use std::io::ErrorKind;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  #[serde(default)]
  pub t: String,
}

// This is extremely fast for creating lots of small objects, as there's no overhead of having to make multiple requests (create + n * write + commit) and using many parallel connections, all of which only have very small transfer sizes. This is really to avoid the overhead of HTTP (pipelining, multiplexing, parsing).
// This endpoint will never return a bad status when processing the objects. However, as soon as there's an error, the processing stops immediately. The amount of successfully created objects is returned in a header, and objects are created in order. This endpoint is not atomic, not even at the individual object level.
pub async fn endpoint_batch_create_objects(
  State(ctx): State<Arc<HttpCtx>>,
  req: Query<InputQueryParams>,
  body: BodyStream,
) -> (StatusCode, HeaderMap) {
  if !ctx.verify_auth(&req.t, AuthTokenAction::BatchCreateObjects {}) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let mut body = body
    .map_err(|err| std::io::Error::new(ErrorKind::Other, err))
    .into_async_read();

  let mut created = 0;

  'outer: loop {
    let mut key_len_raw = vec![0u8; 2];
    let Ok(_) = body.read_exact(&mut key_len_raw).await else {
      break;
    };
    let key_len = key_len_raw.read_u16_be_at(0);

    let mut key = vec![0u8; usz!(key_len)];
    let Ok(_) = body.read_exact(&mut key).await else {
      break;
    };

    let mut size_raw = vec![0u8; 5];
    let Ok(_) = body.read_exact(&mut size_raw).await else {
      break;
    };
    let size = size_raw.read_u40_be_at(0);

    let Ok(creation) = ctx.blobd.create_object(OpCreateObjectInput {
      key: key.into(),
      size,
      #[cfg(feature = "blobd-lite")]
      assoc_data: tinybuf::TinyBuf::empty(),
    }).await else {
      break;
    };

    for offset in (0..size).step_by(usz!(ctx.blobd.cfg().lpage_size())) {
      let mut chunk = vec![0u8; usz!(min(ctx.blobd.cfg().lpage_size(), size - offset))];
      let Ok(_) = body.read_exact(&mut chunk).await else {
        break 'outer;
      };
      let Ok(_) = ctx.blobd.write_object(OpWriteObjectInput {
        data_len: chunk.len().try_into().unwrap(),
        data_stream: once(async { Ok(chunk) }).boxed(),
        incomplete_token: creation.token,
        offset,
      }).await else {
        break 'outer;
      };
    }

    let Ok(_) = ctx.blobd.commit_object(OpCommitObjectInput { incomplete_token: creation.token, if_not_exists: false }).await else {
      break;
    };

    created += 1;
  }

  let mut headers = HeaderMap::new();
  headers.insert("x-blobd-objects-created-count", created.into());
  (StatusCode::OK, headers)
}
