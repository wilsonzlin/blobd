use super::parse_key;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::InodeState;
use crate::inode::INO_OFFSETOF_SIZE;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header::CONTENT_LENGTH;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use off64::Off64Int;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub t: String,
}

pub async fn endpoint_inspect_object(
  State(ctx): State<Arc<Ctx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> (StatusCode, HeaderMap) {
  let (key, key_len) = parse_key(&uri);
  if AuthToken::verify(&ctx.tokens, &req.t, AuthTokenAction::InspectObject {
    key: key.clone(),
  }) {
    return (StatusCode::UNAUTHORIZED, HeaderMap::new());
  };

  let bucket_id = ctx.buckets.bucket_id_for_key(&key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(
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
  headers.insert("x-blobd-object-id", object_id.into());
  (StatusCode::OK, headers)
}
