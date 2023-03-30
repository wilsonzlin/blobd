use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_slots::IncompleteSlotId;
use crate::inode::get_object_alloc_cfg;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_KEY;
use crate::inode::INO_OFFSETOF_KEY_LEN;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_OBJ_ID;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::inode::INO_SIZE;
use crate::op::key_debug_str;
use off64::usz;
use off64::Off64Int;
use off64::Off64Slice;
use std::sync::Arc;
use tracing::trace;
use write_journal::AtomicWriteGroup;

pub struct OpCreateObjectInput {
  pub key: Vec<u8>,
  pub size: u64,
}

pub struct OpCreateObjectOutput {
  pub object_id: u64,
  pub incomplete_slot_id: IncompleteSlotId,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();
  let ObjectAllocCfg {
    tile_count,
    tail_len,
  } = get_object_alloc_cfg(req.size);
  let ino_size = INO_SIZE(key_len, tile_count.into());
  trace!(
    key = key_debug_str(&req.key),
    inode_size = ino_size,
    size = req.size,
    solid_tile_count = tile_count,
    tail_fragment_len = tail_len,
    "creating object"
  );

  let mut writes = Vec::with_capacity(5 + usz!(tile_count));

  let (change_serial, inode_dev_offset, tiles, tail_dev_offset) = {
    let mut free_list = ctx.free_list.lock().await;
    let inode_dev_offset = free_list.allocate_fragment(&mut writes, ino_size.try_into().unwrap());
    let tiles = free_list.allocate_tiles(&mut writes, tile_count);
    let tail_dev_offset = if tail_len == 0 {
      0
    } else {
      free_list.allocate_fragment(&mut writes, tail_len.try_into().unwrap())
    };
    (
      free_list.generate_change_serial(),
      inode_dev_offset,
      tiles,
      tail_dev_offset,
    )
  };
  let object_id = ctx.object_id_serial.next(&mut writes);
  let incomplete_slot_id = ctx
    .incomplete_slots
    .allocate_slot(&mut writes, object_id, inode_dev_offset)
    .await;

  trace!(
    key = key_debug_str(&req.key),
    object_id,
    inode_dev_offset,
    solid_tiles = format!("{:?}", tiles),
    tail_fragment_dev_offset = tail_dev_offset,
    "allocated object"
  );

  let mut ino_raw = vec![0u8; usz!(ino_size)];
  ino_raw.write_u48_be_at(INO_OFFSETOF_NEXT_INODE_DEV_OFFSET, 0);
  ino_raw.write_u48_be_at(INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, tail_dev_offset);
  ino_raw.write_u40_be_at(INO_OFFSETOF_SIZE, req.size);
  ino_raw.write_u64_be_at(INO_OFFSETOF_OBJ_ID, object_id);
  ino_raw.write_u16_be_at(INO_OFFSETOF_KEY_LEN, key_len);
  ino_raw.write_slice_at(INO_OFFSETOF_KEY, &req.key);
  for (tile_idx, &tile_no) in tiles.iter().enumerate() {
    let tile_idx: u16 = tile_idx.try_into().unwrap();
    ino_raw.write_u24_be_at(INO_OFFSETOF_TILE_IDX(key_len, tile_idx), tile_no);
  }

  ctx.device.write_at(inode_dev_offset, ino_raw).await;

  ctx
    .journal
    .write(change_serial, AtomicWriteGroup(writes))
    .await;
  trace!(key = key_debug_str(&req.key), object_id, "object created");

  Ok(OpCreateObjectOutput {
    object_id,
    incomplete_slot_id,
  })
}
