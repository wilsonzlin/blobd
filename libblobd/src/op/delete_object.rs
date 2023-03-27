use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::InodeState;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILES;
use crate::inode::INO_SIZE;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use itertools::Itertools;
use off64::create_u48_be;
use off64::Off64Int;
use std::sync::Arc;
use write_journal::AtomicWriteGroup;

pub struct OpDeleteObjectInput {
  pub key: Vec<u8>,
}

pub struct OpDeleteObjectOutput {}

// We hold write lock on bucket RwLock for entire request (including writes and data sync) for simplicity and avoidance of subtle race conditions. Performance should still be great as one bucket equals one object given desired bucket count and load. If we release lock before we (or journal) finishes writes, we need to prevent/handle any possible intermediate read and write of the state of inode elements on the device, linked list pointers, garbage collectors, premature use of data or reuse of freed space, etc.
pub async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();

  let bkt_id = ctx.buckets.bucket_id_for_key(&req.key);
  let bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let Some(FoundInode { prev_dev_offset: prev_inode, next_dev_offset: next_inode, dev_offset: inode_dev_offset, object_id }) = bkt.find_inode(
    &ctx.buckets,
    bkt_id,
    &req.key,
    key_len,
    InodeState::Ready,
    None,
  ) else {
    return Err(OpError::ObjectNotFound);
  };

  // mmap memory should already be in page cache.
  let object_size = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5)
    .read_u40_be_at(0);
  let alloc_cfg = get_object_alloc_cfg(object_size);
  let tail_dev_offset = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
    .read_u48_be_at(0);
  let tiles = ctx
    .device
    .read_at_sync(
      inode_dev_offset + INO_OFFSETOF_TILES(key_len),
      3 * u64::from(alloc_cfg.tile_count),
    )
    .chunks(3)
    .map(|t| t.read_u24_be_at(0))
    .collect_vec();
  let inode_size = INO_SIZE(key_len, alloc_cfg.tile_count);

  let mut writes = Vec::with_capacity(tiles.len() + 4);

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
      ctx
        .buckets
        .mutate_bucket_head(&mut writes, bkt_id, next_inode.unwrap_or(0));
    }
  };

  // Create stream event.
  ctx
    .stream
    .write()
    .await
    .create_event(&mut writes, StreamEvent {
      typ: StreamEventType::ObjectDelete,
      bucket_id: bkt_id,
      object_id,
    });

  ctx
    .journal
    .lock()
    .await
    .write(change_serial, AtomicWriteGroup(writes))
    .await;

  Ok(OpDeleteObjectOutput {})
}
