use super::OpError;
use super::OpResult;
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
use futures::Stream;
use futures::StreamExt;
use off64::Off64Int;
use std::cmp::min;
use std::error::Error;
use std::sync::Arc;

pub struct OpWriteObjectInput<
  S: Unpin + Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
> {
  pub offset: u64,
  pub object_id: u64,
  pub inode_dev_offset: u64,
  pub data_len: u64,
  pub data_stream: S,
}

pub struct OpWriteObjectOutput {}

// TODO We currently don't verify that the key is correct.
pub async fn op_write_object<
  S: Unpin + Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
>(
  ctx: Arc<Ctx>,
  mut req: OpWriteObjectInput<S>,
) -> OpResult<OpWriteObjectOutput> {
  let len = req.data_len;
  let ino_dev_offset = req.inode_dev_offset;

  if req.offset % u64::from(TILE_SIZE) != 0 {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > u64::from(TILE_SIZE) {
    // Cannot write greater than one tile size in one request.
    return Err(OpError::InexactWriteLength);
  };

  // TODO Optimisation: make one read, or don't use await.

  if ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_STATE, 1)
    .await[0]
    != InodeState::Incomplete as u8
  {
    // Inode has incorrect state.
    return Err(OpError::ObjectNotFound);
  };

  if ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_OBJ_ID, 8)
    .await
    .read_u64_be_at(0)
    != req.object_id
  {
    // Inode has different object ID.
    return Err(OpError::ObjectNotFound);
  };

  let size = ctx
    .device
    .read_at(ino_dev_offset + INO_OFFSETOF_SIZE, 5)
    .await
    .read_u40_be_at(0);
  if req.offset + len > size {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len < min(req.offset + u64::from(TILE_SIZE), size) {
    // Write does not fully fill solid tile or tail data fragment. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
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

  let mut written = 0;
  while let Some(chunk) = req.data_stream.next().await {
    let chunk = chunk.map_err(|err| OpError::DataStreamError(Box::from(err)))?;
    let chunk_len: u64 = chunk.len().try_into().unwrap();
    if written + chunk_len > len {
      return Err(OpError::DataStreamLengthMismatch);
    };
    // Optimisation: fdatasync at end of all writes.
    ctx.device.write_at(write_dev_offset + written, chunk).await;
    written += chunk_len;
  }
  if written != len {
    return Err(OpError::DataStreamLengthMismatch);
  };

  // Optimisation: perform fdatasync in batches.
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  Ok(OpWriteObjectOutput {})
}
