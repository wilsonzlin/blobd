use super::parse_key;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::InodeState;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::tile::TILE_SIZE;
use axum::body::StreamBody;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::Range;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::Response;
use axum::TypedHeader;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use bytes::Bytes;
use futures::Stream;
use itertools::Itertools;
use off64::Off64Int;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::min;
use std::error::Error;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub struct ReadObjectStream {
  ctx: Arc<Ctx>,
  key: Vec<u8>,
  key_len: u16,
  bucket_id: u64,
  bucket_version: u64,
  object_id: u64,
  object_size: u64,
  next: u64,
  end: u64,
  inode_dev_offset: u64,
  alloc_cfg: ObjectAllocCfg,
}

#[derive(Debug, strum::Display)]
pub enum ReadObjectStreamError {}

const STREAM_BUFSIZE: u64 = 1024 * 8;

impl Error for ReadObjectStreamError {}

impl Stream for ReadObjectStream {
  type Item = Result<Bytes, Box<ReadObjectStreamError>>;

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if self.next >= self.end {
      return Poll::Ready(None);
    };
    // We cannot stream bytes directly from mmap, as we'll drop the RwLock after this function and the object might get deleted and its tiles repurposed during that time. Even with a very conservative GC that only frees tiles once fully clear and never moves fragments around, it's still allowed to delete the fragments and tiles once the object is deleted and the object can be deleted as soon as we release the RwLock.
    // This means we have to copy the data into a Vec instead. This should be fine; the bytes will be quickly passed to the kernel, so the Vec will be freed quickly and should not build up memory pressure significantly. We use the `bucket_version` optimisation so we can do repeated small buffer reads (instead of large ones) without much expense.
    // An alternative is to hold the RwLock for the entirety of the request, but this means deletion requests will take much longer, especially if there are many slow clients and it's a large object.
    // It's a shame we cannot simply copy from mmap directly (e.g. `send`) to the kernel.
    let ctx = self.ctx.clone();
    let bkt = ctx.buckets.get_bucket(self.bucket_id).blocking_read();
    if bkt.version != self.bucket_version {
      // TODO Ideally this would be async, however calling async from a poll_next is difficult.
      let Some(f) = bkt.find_inode(
        &ctx.buckets,
        self.bucket_id,
        &self.key,
        self.key_len,
        InodeState::Ready,
        Some(self.object_id),
      ) else {
        return Poll::Ready(None);
      };
      self.inode_dev_offset = f.dev_offset;
      self.bucket_version = bkt.version;
    };

    let tile_idx = u16::try_from(self.next / u64::from(TILE_SIZE)).unwrap();
    let data_dev_offset = if tile_idx < self.alloc_cfg.tile_count {
      // mmap memory should already be in page cache.
      // WARNING: Convert both operand values to u64 separately; do not multiply then convert result, as multiplication may overflow in u32.
      u64::from(
        ctx
          .device
          .read_at_sync(
            self.inode_dev_offset + INO_OFFSETOF_TILE_IDX(self.key_len, tile_idx),
            3,
          )
          .read_u24_be_at(0),
      ) * u64::from(TILE_SIZE)
    } else {
      // mmap memory should already be in page cache.
      ctx
        .device
        .read_at_sync(self.inode_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
        .read_u48_be_at(0)
    };
    assert!(data_dev_offset > 0);

    let max_end = min(
      min(self.end, (u64::from(tile_idx) + 1) * u64::from(TILE_SIZE)),
      self.object_size,
    );
    let end = min(max_end, self.next + STREAM_BUFSIZE);
    // TODO Ideally this would be async, however calling async from a poll_next is difficult.
    let data = ctx.device.read_at_sync(self.next, end - self.next);
    self.next = end;
    Poll::Ready(Some(Ok(Bytes::from(data))))
  }
}

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub t: String,
}

pub async fn endpoint_read_object(
  State(ctx): State<Arc<Ctx>>,
  TypedHeader(range): TypedHeader<Range>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> Result<Response<StreamBody<ReadObjectStream>>, StatusCode> {
  let (key, key_len) = parse_key(&uri);
  if AuthToken::verify(&ctx.tokens, &req.t, AuthTokenAction::ReadObject {
    key: key.clone(),
  }) {
    return Err(StatusCode::UNAUTHORIZED);
  };

  let ranges = range.iter().collect_vec();
  if ranges.len() > 1 {
    return Err(StatusCode::RANGE_NOT_SATISFIABLE);
  };
  let range = ranges
    .first()
    .cloned()
    .unwrap_or((Bound::Unbounded, Bound::Unbounded));

  let bucket_id = ctx.buckets.bucket_id_for_key(&key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let bucket_version = bkt.version;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(
    &ctx.buckets,
    bucket_id,
    &key,
    key_len,
    InodeState::Ready,
    None,
  ) else {
    return Err(StatusCode::NOT_FOUND);
  };
  // mmap memory should already be in page cache.
  let object_size = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5)
    .read_u40_be_at(0);
  let start = match range.0 {
    Bound::Included(v) => v,
    // Lower bound must always be inclusive.
    Bound::Excluded(_) => return Err(StatusCode::RANGE_NOT_SATISFIABLE),
    Bound::Unbounded => 0,
  };
  // Exclusive.
  let end = match range.1 {
    Bound::Included(v) => v + 1,
    Bound::Excluded(v) => v,
    Bound::Unbounded => object_size,
  };
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(StatusCode::RANGE_NOT_SATISFIABLE);
  };

  let alloc_cfg = get_object_alloc_cfg(object_size);
  let stream = ReadObjectStream {
    alloc_cfg,
    bucket_id,
    bucket_version,
    ctx: ctx.clone(),
    end,
    inode_dev_offset,
    key_len,
    key,
    next: start,
    object_id,
    object_size,
  };

  Ok(
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
        format!("bytes {start}-{end}/{object_size}"),
      )
      .header("x-blobd-object-id", object_id.to_string())
      .body(StreamBody::new(stream))
      .unwrap(),
  )
}
