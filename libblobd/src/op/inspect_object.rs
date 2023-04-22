use super::OpError;
use super::OpResult;
use crate::bucket::FoundInode;
use crate::ctx::Ctx;
use crate::object::OBJECT_OFF;
use off64::int::Off64AsyncReadInt;
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
  let bkt = ctx.buckets.get_bucket_for_key(&req.key).await;
  let Some(FoundInode { dev_offset: inode_dev_offset, object_id, .. }) = bkt.find_inode(None).await else {
    return Err(OpError::ObjectNotFound);
  };
  let object_size = ctx
    .device
    .read_u40_be_at(inode_dev_offset + OBJECT_OFF.size())
    .await;

  Ok(OpInspectObjectOutput {
    object_id,
    size: object_size,
  })
}
