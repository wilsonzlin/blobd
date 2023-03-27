use super::UploadId;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::InodeState;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_KEY_LEN;
use crate::inode::INO_OFFSETOF_OBJ_ID;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_STATE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::tile::TILE_SIZE;
use axum::extract::BodyStream;
use axum::extract::Query;
use axum::extract::State;
use axum::headers::ContentLength;
use axum::http::StatusCode;
use axum::TypedHeader;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use futures::StreamExt;
use off64::Off64Int;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::min;
use std::io::Read;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub offset: u64,
  pub object_id: u64,
  pub upload_id: String,
  pub t: String,
}

// TODO We currently don't verify that the key is correct.
pub async fn endpoint_write_object(
  State(ctx): State<Arc<Ctx>>,
  TypedHeader(ContentLength(len)): TypedHeader<ContentLength>,
  req: Query<InputQueryParams>,
  mut body: BodyStream,
) -> StatusCode {
  if AuthToken::verify(&ctx.tokens, &req.t, AuthTokenAction::WriteObject {
    object_id: req.object_id,
    offset: req.offset,
  }) {
    return StatusCode::UNAUTHORIZED;
  };

  if req.offset % u64::from(TILE_SIZE) != 0 {
    // Invalid offset.
    return StatusCode::RANGE_NOT_SATISFIABLE;
  };
  if len > u64::from(TILE_SIZE) {
    // Cannot write greater than one tile size in one request.
    return StatusCode::PAYLOAD_TOO_LARGE;
  };
  let Some(ino_dev_offset) = UploadId::parse_and_verify(&ctx.tokens, &req.upload_id) else {
    return StatusCode::NOT_FOUND;
  };

  // TODO Optimisation: make one read, or don't use await.

  if ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_STATE, 1)
    .await[0]
    != InodeState::Incomplete as u8
  {
    // Inode has incorrect state.
    return StatusCode::NOT_FOUND;
  };

  if ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_OBJ_ID, 8)
    .await
    .read_u64_be_at(0)
    != req.object_id
  {
    // Inode has different object ID.
    return StatusCode::NOT_FOUND;
  };

  let size = ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_SIZE, 5)
    .await
    .read_u40_be_at(0);
  if req.offset + len > size {
    // Offset is past size.
    return StatusCode::RANGE_NOT_SATISFIABLE;
  };

  if req.offset + len < min(req.offset + u64::from(TILE_SIZE), size) {
    // Write does not fully fill solid tile or tail data fragment. All writes must fill as otherwise uninitialised data will get exposed.
    return StatusCode::RANGE_NOT_SATISFIABLE;
  };

  let key_len = ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_KEY_LEN, 2)
    .await
    .read_u16_be_at(0);

  let ObjectAllocCfg { tile_count, .. } = get_object_alloc_cfg(size);
  let write_dev_offset = {
    let tile_idx = u16::try_from(req.offset / u64::from(TILE_SIZE)).unwrap();
    debug_assert!(tile_idx <= tile_count);
    if tile_idx < tile_count {
      // WARNING: Convert both operand values to u64 separately; do not multiply then convert result, as multiplication may overflow in u32.
      u64::from(
        ctx
          .device
          .read_at(ino_dev_offset + INO_OFFSETOF_TILE_IDX(key_len, tile_idx), 3)
          .await
          .read_u24_be_at(0),
      ) * u64::from(TILE_SIZE)
    } else {
      ctx
        .device
        .read_at(ino_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
        .await
        .read_u48_be_at(0)
    }
  };

  let mut cur = write_dev_offset;
  while let Some(chunk) = body.next().await {
    let Ok(chunk) = chunk else {
      // TODO
      return StatusCode::REQUEST_TIMEOUT;
    };
    let chunk_len = chunk.len();
    // Optimisation: fdatasync at end of all writes.
    let mut bytes = vec![];
    for seq in chunk.bytes() {
      let Ok(seq) = seq else {
        // TODO
        return StatusCode::REQUEST_TIMEOUT;
      };
      bytes.push(seq);
    }
    ctx.device.write_at(cur, bytes).await;
    cur += u64::try_from(chunk_len).unwrap();
  }

  // Optimisation: perform fdatasync in batches.
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  StatusCode::ACCEPTED
}
