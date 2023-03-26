use std::sync::Arc;

use axum::{extract::{State, Query}, http::{Uri, StatusCode}};
use off64::{Off64Int, usz, create_u48_be};
use serde::{Deserialize, Serialize};

use crate::{ctx::Ctx, inode::{INO_OFFSETOF_STATE, INO_OFFSETOF_KEY_LEN, INO_OFFSETOF_OBJ_ID, InodeState, INO_OFFSETOF_NEXT_INODE_DEV_OFFSET}};

use super::parse_key;


#[derive(Serialize, Deserialize)]
pub struct InputQueryParams {
  pub object_id: u64,
  pub upload_id: u64,
}

pub async fn endpoint_commit_object(
  State(ctx): State<Arc<Ctx>>,
  uri: Uri,
  req: Query<InputQueryParams>,
) -> StatusCode {
  let (key, _) = parse_key(&uri);
  let bkt_id = ctx.buckets.bucket_id_for_key(&key);
  let _bkt = ctx.buckets.get_bucket(bkt_id).write().await;
  let inode_dev_offset = req.upload_id;

  // Check AFTER acquiring lock in case two requests try to commit the same inode.
  {
    let base = INO_OFFSETOF_STATE;
    let raw = ctx.device.read_at(inode_dev_offset + base, INO_OFFSETOF_KEY_LEN - base).await;
    let state = raw[usz!(INO_OFFSETOF_STATE - base)];
    let obj_id = raw.read_u64_be_at(INO_OFFSETOF_OBJ_ID - base);
    if state != InodeState::Incomplete as u8 || obj_id != req.object_id {
      return StatusCode::NOT_FOUND;
    };
  };

  let mut writes = Vec::with_capacity(3);

  // Update bucket head to point to this new inode.
  let cur_bkt_head = ctx.buckets.get_bucket_head(bkt_id).await;
  ctx.buckets.mutate_bucket_head(&mut writes, bkt_id, inode_dev_offset);

  // Update inode state.
  writes.push((
    inode_dev_offset + INO_OFFSETOF_STATE,
    vec![InodeState::Ready as u8],
  ));

  // Update inode next pointer.
  writes.push((
    inode_dev_offset + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET,
    create_u48_be(cur_bkt_head).to_vec(),
  ));

  StatusCode::OK
}
