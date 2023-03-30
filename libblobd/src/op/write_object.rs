use super::OpError;
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
use crate::op::key_debug_str;
use crate::tile::TILE_SIZE_U64;
use futures::Stream;
use futures::StreamExt;
use off64::Off64Int;
use std::cmp::min;
use std::error::Error;
use std::sync::Arc;
use tracing::trace;

pub struct OpWriteObjectInput<
  S: Unpin + Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
> {
  pub offset: u64,
  pub object_id: u64,
  pub incomplete_slot_id: IncompleteSlotId,
  pub data_len: u64,
  pub data_stream: S,
}

pub struct OpWriteObjectOutput {}

// TODO We currently don't verify that the key is correct.
pub(crate) async fn op_write_object<
  S: Unpin + Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
>(
  ctx: Arc<Ctx>,
  mut req: OpWriteObjectInput<S>,
) -> OpResult<OpWriteObjectOutput> {
  let Some(incomplete_slot_lock) = ctx.incomplete_slots.lock_slot_for_writing(req.incomplete_slot_id, req.object_id).await else {
    return Err(OpError::ObjectNotFound);
  };

  let len = req.data_len;
  let ino_dev_offset = incomplete_slot_lock.inode_dev_offset();
  trace!(
    object_id = req.object_id,
    inode_dev_offset = ino_dev_offset,
    offset = req.offset,
    length = req.data_len,
    "writing object"
  );

  if req.offset % TILE_SIZE_U64 != 0 {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > TILE_SIZE_U64 {
    // Cannot write greater than one tile size in one request.
    return Err(OpError::InexactWriteLength);
  };

  let base = INO_OFFSETOF_SIZE;
  let raw = ctx
    .device
    .read_at(
      ino_dev_offset + base,
      INO_OFFSETOF_NEXT_INODE_DEV_OFFSET - base,
    )
    .await;
  let object_id = raw.read_u64_be_at(INO_OFFSETOF_OBJ_ID - base);
  let size = raw.read_u40_be_at(INO_OFFSETOF_SIZE - base);
  let key_len = raw.read_u16_be_at(INO_OFFSETOF_KEY_LEN - base);
  trace!(
    object_id = req.object_id,
    inode_dev_offset = ino_dev_offset,
    size,
    key = key_debug_str(
      &ctx
        .device
        .read_at_sync(ino_dev_offset + INO_OFFSETOF_KEY, key_len.into())
    ),
    "found object to write to"
  );

  if object_id != req.object_id {
    // Inode has different object ID.
    return Err(OpError::ObjectNotFound);
  };

  if req.offset + len > size {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len != min(req.offset + TILE_SIZE_U64, size) {
    // Write does not fully fill solid tile or tail data fragment. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

  let ObjectAllocCfg { tile_count, .. } = get_object_alloc_cfg(size);
  let write_dev_offset = {
    let tile_idx = u16::try_from(req.offset / TILE_SIZE_U64).unwrap();
    debug_assert!(tile_idx <= tile_count);
    if tile_idx < tile_count {
      u64::from(
        // mmap data should already be in page cache.
        ctx
          .device
          .read_at_sync(ino_dev_offset + INO_OFFSETOF_TILE_IDX(key_len, tile_idx), 3)
          .read_u24_be_at(0),
      ) * TILE_SIZE_U64
    } else {
      // mmap data should already be in page cache.
      ctx
        .device
        .read_at_sync(ino_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
        .read_u48_be_at(0)
    }
  };

  let mut written = 0;
  while let Some(chunk) = req.data_stream.next().await {
    let chunk = chunk.map_err(|err| OpError::DataStreamError(Box::from(err)))?;
    let chunk_len: u64 = chunk.len().try_into().unwrap();
    if written + chunk_len > len {
      return Err(OpError::DataStreamLengthMismatch);
    };
    // Optimisation: fdatasync at end of all writes.
    ctx.device.write_at(write_dev_offset + written, chunk).await;
    trace!(
      object_id,
      write_dev_offset = write_dev_offset + written,
      chunk_len,
      "wrote chunk"
    );
    written += chunk_len;
  }
  if written != len {
    return Err(OpError::DataStreamLengthMismatch);
  };

  // Optimisation: perform fdatasync in batches.
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  Ok(OpWriteObjectOutput {})
}
