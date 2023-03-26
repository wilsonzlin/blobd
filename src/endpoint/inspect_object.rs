use super::parse_key;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::InodeState;
use crate::inode::INO_OFFSETOF_SIZE;
use axum::extract::State;
use axum::http::header::CONTENT_LENGTH;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Uri;
use off64::Off64Int;
use std::sync::Arc;

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
  let object_size = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5)
    .read_u40_be_at(0);

  let mut headers = HeaderMap::new();
  headers.insert(CONTENT_LENGTH, object_size.into());
  (StatusCode::OK, headers)
}
