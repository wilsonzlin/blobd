use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::get_object_alloc_cfg;
use crate::inode::InodeState;
use crate::inode::ObjectAllocCfg;
use crate::inode::INO_OFFSETOF_SIZE;
use crate::inode::INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_TILE_IDX;
use crate::tile::TILE_SIZE;
use crate::tile::TILE_SIZE_U64;
use futures::Stream;
use off64::Off64Int;
use std::cmp::min;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub struct ReadObjectStream {
  buf_size: u64,
  ctx: Arc<Ctx>,
  key: Vec<u8>,
  key_len: u16,
  bucket_id: u64,
  bucket_version: u64,
  object_id: u64,
  object_size: u64,
  next: u64,
  end: u64,
  inode_dev_offset: u64,
  alloc_cfg: ObjectAllocCfg,
}

#[derive(Debug, strum::Display)]
pub enum ReadObjectStreamError {}

impl Error for ReadObjectStreamError {}

impl Stream for ReadObjectStream {
  type Item = Result<Vec<u8>, Box<ReadObjectStreamError>>;

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if self.next >= self.end {
      return Poll::Ready(None);
    };
    // We cannot stream bytes directly from mmap, as we'll drop the RwLock after this function and the object might get deleted and its tiles repurposed during that time. Even with a very conservative GC that only frees tiles once fully clear and never moves fragments around, it's still allowed to delete the fragments and tiles once the object is deleted and the object can be deleted as soon as we release the RwLock.
    // This means we have to copy the data into a Vec instead. This should be fine; the bytes will be quickly passed to the kernel, so the Vec will be freed quickly and should not build up memory pressure significantly. We use the `bucket_version` optimisation so we can do repeated small buffer reads (instead of large ones) without much expense.
    // An alternative is to hold the RwLock for the entirety of the request, but this means deletion requests will take much longer, especially if there are many slow clients and it's a large object.
    // It's a shame we cannot simply copy from mmap directly (e.g. `send`) to the kernel.
    let ctx = self.ctx.clone();
    let bkt = ctx.buckets.get_bucket(self.bucket_id).blocking_read();
    if bkt.version != self.bucket_version {
      // TODO Ideally this would be async, however calling async from a poll_next is difficult.
      let Some(f) = bkt.find_inode(
        &ctx.buckets,
        self.bucket_id,
        &self.key,
        self.key_len,
        InodeState::Ready,
        Some(self.object_id),
      ) else {
        return Poll::Ready(None);
      };
      self.inode_dev_offset = f.dev_offset;
      self.bucket_version = bkt.version;
    };

    let tile_idx = u16::try_from(self.next / u64::from(TILE_SIZE)).unwrap();
    let data_dev_offset = if tile_idx < self.alloc_cfg.tile_count {
      // mmap memory should already be in page cache.
      u64::from(
        ctx
          .device
          .read_at_sync(
            self.inode_dev_offset + INO_OFFSETOF_TILE_IDX(self.key_len, tile_idx),
            3,
          )
          .read_u24_be_at(0),
      ) * TILE_SIZE_U64
    } else {
      // mmap memory should already be in page cache.
      ctx
        .device
        .read_at_sync(self.inode_dev_offset + INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET, 6)
        .read_u48_be_at(0)
    };
    assert!(data_dev_offset > 0);

    let max_end = min(
      min(self.end, (u64::from(tile_idx) + 1) * TILE_SIZE_U64),
      self.object_size,
    );
    let end = min(max_end, self.next + self.buf_size);
    // TODO Ideally this would be async, however calling async from a poll_next is difficult.
    let data = ctx.device.read_at_sync(self.next, end - self.next);
    self.next = end;
    Poll::Ready(Some(Ok(data)))
  }
}

pub struct OpReadObjectInput {
  pub key: Vec<u8>,
  pub start: Option<u64>,
  // Exclusive.
  pub end: Option<u64>,
  pub stream_buffer_size: u64,
}

pub struct OpReadObjectOutput {
  pub data_stream: ReadObjectStream,
  pub start: u64,
  pub end: u64,
  pub object_size: u64,
  pub object_id: u64,
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();

  let bucket_id = ctx.buckets.bucket_id_for_key(&req.key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let bucket_version = bkt.version;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(
    &ctx.buckets,
    bucket_id,
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
  let start = req.start.unwrap_or(0);
  // Exclusive.
  let end = req.end.unwrap_or(object_size);
  // Note: disallow empty ranges.
  if start >= end || start >= object_size || end > object_size {
    return Err(OpError::RangeOutOfBounds);
  };

  let alloc_cfg = get_object_alloc_cfg(object_size);
  let stream = ReadObjectStream {
    alloc_cfg,
    buf_size: req.stream_buffer_size,
    bucket_id,
    bucket_version,
    ctx: ctx.clone(),
    end,
    inode_dev_offset,
    key_len,
    key: req.key,
    next: start,
    object_id,
    object_size,
  };

  Ok(OpReadObjectOutput {
    data_stream: stream,
    end,
    object_id,
    object_size,
    start,
  })
}
