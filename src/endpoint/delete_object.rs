use std::sync::Arc;

use axum::{extract::State, http::{Uri, StatusCode}};
use itertools::Itertools;
use off64::{Off64Int, create_u48_be};
use write_journal::AtomicWriteGroup;

use crate::{ctx::Ctx, inode::{InodeState, INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, get_object_alloc_cfg, INO_OFFSETOF_SIZE, INO_OFFSETOF_TILES, INO_OFFSETOF_STATE, INO_OFFSETOF_NEXT_INODE_DEV_OFFSET, INO_SIZE}, bucket::{FoundInode}};

use super::parse_key;


pub async fn endpoint_delete_object(
  State(ctx): State<Arc<Ctx>>,
  uri: Uri,
) -> StatusCode {
  let (key, key_len) = parse_key(&uri);
  let bkt_id = ctx.buckets.bucket_id_for_key(&key);
  let bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let Some(FoundInode { prev_dev_offset: prev_inode, next_dev_offset: next_inode, dev_offset: inode_dev_offset, .. }) = bkt.find_inode(
    &ctx.buckets,
    bkt_id,
    &key,
    key_len,
    InodeState::Ready,
    None,
  ) else {
    return StatusCode::NOT_FOUND;
  };

  // mmap memory should already be in page cache.
  let object_size = ctx.device.read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5).read_u40_be_at(0);
  let alloc_cfg = get_object_alloc_cfg(object_size);
  let tail_dev_offset = ctx.device.read_at_sync(inode_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6).read_u48_be_at(0);
  let tiles = ctx.device.read_at_sync(inode_dev_offset + INO_OFFSETOF_TILES(key_len), 3 * u64::from(alloc_cfg.tile_count)).chunks(3).map(|t| t.read_u24_be_at(0)).collect_vec();
  let inode_size = INO_SIZE(key_len, alloc_cfg.tile_count);

  let mut writes = Vec::with_capacity(tiles.len() + 5);

  // Release allocated space.
  let change_serial = {
    let mut free_list = ctx.free_list.lock().await;
    free_list.release_tiles(&mut writes, &tiles);
    free_list.release_fragment(&mut writes, inode_dev_offset, alloc_cfg.tail_len);
    if tail_dev_offset != 0 {
      free_list.release_fragment(&mut writes, tail_dev_offset, inode_size);
    };
    free_list.generate_change_serial()
  };

  // Update inode state.
  writes.push((inode_dev_offset + INO_OFFSETOF_STATE, vec![InodeState::Deleted as u8]));
  match prev_inode {
    Some(prev_inode_dev_offset) => {
      // Update next pointer of previous inode.
      writes.push((
        prev_inode_dev_offset + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET,
        create_u48_be(next_inode.unwrap_or(0)).to_vec(),
      ));
    }
    None => {
      // Update bucket head.
      ctx.buckets.mutate_bucket_head(&mut writes, bkt_id, next_inode.unwrap_or(0));
    }
  };

  ctx.journal.lock().await.write(change_serial, AtomicWriteGroup(writes)).await;

  StatusCode::OK
}
