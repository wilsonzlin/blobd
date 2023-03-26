use super::parse_key;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::InodeState;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_KEY;
use crate::inode::INO_OFFSETOF_KEY_LEN;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_OBJ_ID;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_STATE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::inode::INO_SIZE;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::Json;
use off64::usz;
use off64::Off64Int;
use off64::Off64Slice;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use write_journal::AtomicWriteGroup;

#[derive(Serialize, Deserialize)]
pub struct Input {
  pub size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Output {
  object_id: u64,
  upload_id: u64,
}

pub async fn endpoint_create_object(
  State(ctx): State<Arc<Ctx>>,
  uri: Uri,
  Json(req): Json<Input>,
) -> Result<Json<Output>, StatusCode> {
  let (key, key_len) = parse_key(&uri);
  let ObjectAllocCfg {
    tile_count,
    tail_len,
  } = get_object_alloc_cfg(req.size);
  let ino_size = INO_SIZE(key_len, tile_count.into());

  let mut writes = Vec::with_capacity(4 + usz!(tile_count));

  let (change_serial, ino_dev_offset, tiles, tail_dev_offset) = {
    let mut free_list = ctx.free_list.lock().await;
    let ino_dev_offset = free_list.allocate_fragment(&mut writes, ino_size.try_into().unwrap());
    let tiles = free_list.allocate_tiles(&mut writes, tile_count);
    let tail_dev_offset = if tail_len == 0 {
      0
    } else {
      free_list.allocate_fragment(&mut writes, tail_len.try_into().unwrap())
    };
    (
      free_list.generate_change_serial(),
      ino_dev_offset,
      tiles,
      tail_dev_offset,
    )
  };

  let object_id = ctx.object_id_serial.next(&mut writes);

  let mut ino_raw = vec![0u8; usz!(ino_size)];
  ino_raw.write_u48_be_at(INO_OFFSETOF_NEXT_INODE_DEV_OFFSET, 0);
  ino_raw[usz!(INO_OFFSETOF_STATE)] = InodeState::Incomplete as u8;
  ino_raw.write_u48_be_at(INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, tail_dev_offset);
  ino_raw.write_u40_be_at(INO_OFFSETOF_SIZE, req.size);
  ino_raw.write_u64_be_at(INO_OFFSETOF_OBJ_ID, object_id);
  ino_raw.write_u16_be_at(INO_OFFSETOF_KEY_LEN, key_len);
  ino_raw.write_slice_at(INO_OFFSETOF_KEY, &key);
  for (tile_idx, &tile_no) in tiles.iter().enumerate() {
    let tile_idx: u16 = tile_idx.try_into().unwrap();
    ino_raw.write_u24_be_at(INO_OFFSETOF_TILE_IDX(key_len, tile_idx), tile_no);
  }

  // The inode must be journalled as well, as the GC depends on consistent data when reading raw tiles directly.
  writes.push((ino_dev_offset, ino_raw));

  ctx
    .journal
    .lock()
    .await
    .write(change_serial, AtomicWriteGroup(writes))
    .await;

  Ok(Json(Output {
    object_id,
    upload_id: ino_dev_offset,
  }))
}
