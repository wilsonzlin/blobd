use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::inode::InodeState;
use crate::inode::INO_OFFSETOF_SIZE;
use off64::Off64Int;
use std::sync::Arc;

pub struct OpInspectObjectInput {
  pub key: Vec<u8>,
}

pub struct OpInspectObjectOutput {
  pub size: u64,
  pub object_id: u64,
}

pub(crate) async fn op_inspect_object(
  ctx: Arc<Ctx>,
  req: OpInspectObjectInput,
) -> OpResult<OpInspectObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();

  let bucket_id = ctx.buckets.bucket_id_for_key(&req.key);
  let bkt = ctx.buckets.get_bucket(bucket_id).read().await;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(
    &ctx.buckets,
    bucket_id,
    &req.key,
    key_len,
    InodeState::Ready,
    None,
  ).await else {
    return Err(OpError::ObjectNotFound);
  };
  // mmap memory should already be in page cache.
  let object_size = ctx
    .device
    .read_at_sync(inode_dev_offset + INO_OFFSETOF_SIZE, 5)
    .read_u40_be_at(0);

  Ok(OpInspectObjectOutput {
    object_id,
    size: object_size,
  })
}
