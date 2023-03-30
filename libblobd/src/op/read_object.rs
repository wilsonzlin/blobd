use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::op::key_debug_str;
use crate::tile::TILE_SIZE_U64;
use futures::Stream;
use off64::Off64Int;
use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use tracing::trace;

struct ReadObjectStreamCfg {
  buf_size: u64,
  ctx: Arc<Ctx>,
  key: Vec<u8>,
  bucket_id: u64,
  bucket_version: u64,
  object_id: u64,
  object_size: u64,
  start: u64,
  end: u64,
  inode_dev_offset: u64,
  alloc_cfg: ObjectAllocCfg,
}

// We cannot stream bytes directly from mmap, as we'll drop the RwLock after this function and the object might get deleted and its tiles repurposed during that time. Even with a very conservative GC that only frees tiles once fully clear and never moves fragments around, it's still allowed to delete the fragments and tiles once the object is deleted and the object can be deleted as soon as we release the RwLock.
// This means we have to copy the data into a Vec instead. This should be fine; the bytes will be quickly passed to the kernel, so the Vec will be freed quickly and should not build up memory pressure significantly. We use the `bucket_version` optimisation so we can do repeated small buffer reads (instead of large ones) without much expense.
// An alternative is to hold the RwLock for the entirety of the request, but this means deletion requests will take much longer, especially if there are many slow clients and it's a large object.
// It's a shame we cannot simply copy from mmap directly (e.g. `send`) to the kernel.
fn create_read_object_stream(
  ReadObjectStreamCfg {
    alloc_cfg,
    bucket_id,
    mut bucket_version,
    buf_size,
    ctx,
    end,
    mut inode_dev_offset,
    key,
    start,
    object_id,
    object_size,
  }: ReadObjectStreamCfg,
) -> impl Stream<Item = Vec<u8>> {
  let key_len: u16 = key.len().try_into().unwrap();
  let mut chunk_start = start;
  async_stream::stream! {
    loop {
      if chunk_start >= end {
        trace!(key = key_debug_str(&key), object_id, object_size, start, chunk_start, end, "no more data to stream");
        break;
      };
      let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
      if bkt.version != bucket_version {
        trace!(key = key_debug_str(&key), object_id, object_size, start, chunk_start, end, "bucket has changed, looking up object again using key");
        let Some(f) = bkt.find_inode(
          &ctx.buckets,
          bucket_id,
          &key,
          key_len,
          Some(object_id),
        ).await else {
          break;
        };
        inode_dev_offset = f.dev_offset;
        bucket_version = bkt.version;
      };

      let tile_idx = u16::try_from(chunk_start / TILE_SIZE_U64).unwrap();
      // The device offset of the current tile or tail data fragment (`data_dev_offset`) changes each TILE_SIZE, so this is not the same as `chunk_start`. Think of `chunk_start` as the virtual pointer within a contiguous span of the object's data bytes, and this as the physical offset within the physical tile that backs the current position of the virtual pointer within the object's data made from many physical tiles + tail data fragment.
      let chunk_start_within_tile = chunk_start % TILE_SIZE_U64;
      // This is the device offset of the current tile or tail data fragment that contains the data at the current `chunk_start`.
      let data_dev_offset = if tile_idx < alloc_cfg.tile_count {
        // mmap memory should already be in page cache.
        u64::from(
          ctx
            .device
            .read_at_sync(
              inode_dev_offset + INO_OFFSETOF_TILE_IDX(key_len, tile_idx),
              3,
            )
            .read_u24_be_at(0),
        ) * TILE_SIZE_U64
      } else {
        // mmap memory should already be in page cache.
        ctx
          .device
          .read_at_sync(inode_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
          .read_u48_be_at(0)
      };
      assert!(data_dev_offset > 0);

      // Can't read past current tile, as we'll need to switch to a different tile then.
      let max_chunk_end = min(
        end,
        (u64::from(tile_idx) + 1) * TILE_SIZE_U64,
      );
      let chunk_end = min(max_chunk_end, chunk_start + buf_size);
      let chunk_len = chunk_end - chunk_start;
      let read_dev_offset = data_dev_offset + chunk_start_within_tile;
      trace!(key = key_debug_str(&key), object_id, object_size, start, chunk_start, chunk_end, end, read_dev_offset, chunk_len, "streaming data");
      let data = ctx.device.read_at(read_dev_offset, chunk_len).await;
      chunk_start = chunk_end;
      yield data;
    };
  }
}

pub struct OpReadObjectInput {
  pub key: Vec<u8>,
  pub start: u64,
  // Exclusive.
  pub end: Option<u64>,
  pub stream_buffer_size: u64,
}

pub struct OpReadObjectOutput {
  pub data_stream: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
  pub start: u64,
  pub end: u64,
  pub object_size: u64,
  pub object_id: u64,
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  trace!(
    key = key_debug_str(&req.key),
    start = req.start,
    end = req.end,
    "reading object"
  );
  let key_len: u16 = req.key.len().try_into().unwrap();

  let bucket_id = ctx.buckets.bucket_id_for_key(&req.key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let bucket_version = bkt.version;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(
    &ctx.buckets,
    bucket_id,
    &req.key,
    key_len,
    None,
  ).await else {
    return Err(OpError::ObjectNotFound);
  };
  // mmap memory should already be in page cache.
  let object_size = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5)
    .read_u40_be_at(0);
  let start = req.start;
  // Exclusive.
  let end = req.end.unwrap_or(object_size);
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(OpError::RangeOutOfBounds);
  };
  trace!(
    key = key_debug_str(&req.key),
    inode_dev_offset,
    object_id,
    object_size,
    start,
    end,
    "found object to read"
  );

  let alloc_cfg = get_object_alloc_cfg(object_size);
  let stream_cfg = ReadObjectStreamCfg {
    alloc_cfg,
    bucket_id,
    bucket_version,
    buf_size: req.stream_buffer_size,
    ctx: ctx.clone(),
    end,
    inode_dev_offset,
    key: req.key,
    object_id,
    object_size,
    start,
  };

  Ok(OpReadObjectOutput {
    data_stream: Box::pin(create_read_object_stream(stream_cfg)),
    end,
    object_id,
    object_size,
    start,
  })
}
