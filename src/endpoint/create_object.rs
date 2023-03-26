use std::sync::Arc;

use axum::{http::{StatusCode, Uri}, extract::State, Json};
use off64::{Off64, usz};
use serde::{Serialize, Deserialize};
use write_journal::AtomicWriteGroup;

use crate::{ctx::Ctx, tile::TILE_SIZE, inode::{INO_SIZE, INO_OFFSETOF_FRAG_TAG, INO_OFFSETOF_NEXT_INODE_DEV_OFFSET, INO_OFFSETOF_STATE, InodeState, INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, INO_OFFSETOF_SIZE, INO_OFFSETOF_OBJ_ID, INO_OFFSETOF_KEY_LEN, INO_OFFSETOF_KEY, INO_OFFSETOF_TILE_IDX, ObjectAllocCfg, get_object_alloc_cfg}, free_list::{FREELIST_OFFSETOF_TILE, FragmentType}};

use super::parse_key;

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
  let ObjectAllocCfg { tile_count, tail_len } = get_object_alloc_cfg(req.size);
  let ino_size = INO_SIZE(key_len, tile_count.into());

  let (change_serial, ino_dev_offset, ino_tile_new_usage, tiles, tail_dev_offset, tail_tile_new_usage) = {
    let mut free_list = ctx.free_list.lock().await;
    let (ino_dev_offset, ino_tile_new_usage) = free_list.allocate_fragment(ino_size.try_into().unwrap());
    let tiles = free_list.allocate_tiles(tile_count);
    let (tail_dev_offset, tail_tile_new_usage) = if tail_len == 0 {
      (0, 0)
    } else {
      free_list.allocate_fragment(tail_len.try_into().unwrap())
    };
    (free_list.generate_change_serial(), ino_dev_offset, ino_tile_new_usage, tiles, tail_dev_offset, tail_tile_new_usage)
  };

  // TODO
  let object_id = 0;

  let mut ino_raw = vec![0u8; usz!(ino_size)];
  ino_raw[usz!(INO_OFFSETOF_FRAG_TAG)] = FragmentType::Inode as u8;
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
  };

  let mut writes = vec![];

  // Update inode fragmented tile space usage.
  let mut inode_tile_freelist_update = vec![0u8; 3];
  inode_tile_freelist_update.write_u24_be_at(0, ino_tile_new_usage);
  writes.push((
    ctx.free_list_dev_offset + FREELIST_OFFSETOF_TILE((ino_dev_offset / u64::from(TILE_SIZE)).try_into().unwrap()),
    inode_tile_freelist_update,
  ));

  // Update tail data fragmented tile space usage.
  let mut tail_tile_freelist_update = vec![0u8; 3];
  tail_tile_freelist_update.write_u24_be_at(0, tail_tile_new_usage);
  writes.push((
    ctx.free_list_dev_offset + FREELIST_OFFSETOF_TILE((tail_dev_offset / u64::from(TILE_SIZE)).try_into().unwrap()),
    tail_tile_freelist_update,
  ));

  // Update solid tiles space usage.
  for &tile_no in tiles.iter() {
    let mut update = vec![0u8; 3];
    update.write_u24_be_at(0, TILE_SIZE);
    writes.push((
      ctx.free_list_dev_offset + FREELIST_OFFSETOF_TILE(tile_no),
      update
    ));
  }

  // The inode must be journalled as well, as the GC depends on consistent data when reading raw tiles directly.
  writes.push((
    ino_dev_offset,
    ino_raw,
  ));

  ctx.journal.lock().await.write(change_serial, AtomicWriteGroup(writes)).await;

  Ok(Json(Output {
    object_id,
    upload_id: ino_dev_offset,
   }))
}
