use std::{error::Error, pin::Pin, task::{Context, Poll}, sync::Arc, fmt::Display, ops::Bound, cmp::min};

use axum::{http::{StatusCode, Uri, HeaderMap, header::CONTENT_LENGTH}, extract::State, Json, TypedHeader, headers::Range, body::StreamBody};
use bytes::Bytes;
use futures::{TryStream, Stream};
use itertools::Itertools;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;

use crate::{ctx::Ctx, bucket::{Buckets, FoundInode}, inode::{InodeState, get_object_alloc_cfg, INO_OFFSETOF_SIZE, ObjectAllocCfg, INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, INO_OFFSETOF_TILE_IDX}, tile::TILE_SIZE};

use super::parse_key;

pub async fn endpoint_inspect_object(
  State(ctx): State<Arc<Ctx>>,
  uri: Uri,
) -> (StatusCode, HeaderMap) {
  let (key, key_len) = parse_key(&uri);
  let bucket_id = ctx.buckets.bucket_id_for_key(&key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let Some(FoundInode { dev_offset: inode_dev_offset, .. }) = bkt.find_inode(
    &ctx.buckets,
    bucket_id,
    &key,
    key_len,
    InodeState::Ready,
    None,
  ) else {
    return (StatusCode::NOT_FOUND, HeaderMap::new());
  };
  // mmap memory should already be in page cache.
  let object_size = ctx.device.read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5).read_u40_be_at(0);

  let mut headers = HeaderMap::new();
  headers.insert(CONTENT_LENGTH, object_size.into());
  (StatusCode::OK, headers)
}
