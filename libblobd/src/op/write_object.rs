use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::inode::calc_inode_layout;
use crate::inode::InodeLayout;
use crate::inode::INODE_OFF;
use crate::op::key_debug_str;
use crate::page::IncompleteInodePageHeader;
use crate::util::div_pow2;
use crate::util::is_multiple_of_pow2;
use chrono::Utc;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64ReadInt;
use off64::u32;
use off64::u64;
use off64::usz;
use std::cmp::min;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::trace;

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
// TODO The caller must ensure that the object exists, otherwise corruption could occur. For example, use a signed token that expires well before any incomplete object would be reaped, and do not allow direct deletions of incomplete objects.
pub(crate) async fn op_write_object<
  S: Unpin + Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
>(
  ctx: Arc<Ctx>,
  mut req: OpWriteObjectInput<S>,
) -> OpResult<OpWriteObjectOutput> {
  let len = req.data_len;
  let inode_dev_offset = req.inode_dev_offset;
  trace!(
    object_id = req.object_id,
    inode_dev_offset,
    offset = req.offset,
    length = req.data_len,
    "writing object"
  );

  let hdr = ctx
    .pages
    .read_page_header::<IncompleteInodePageHeader>(inode_dev_offset)
    .await;

  let incomplete_object_has_expired = || {
    let now_hour = u32!(Utc::now().timestamp() / 60 / 60);
    now_hour - hdr.created_hour <= ctx.incomplete_objects_expire_after_hours
  };

  if incomplete_object_has_expired() {
    return Err(OpError::ObjectNotFound);
  };

  if !is_multiple_of_pow2(req.offset, ctx.pages.lpage_size_pow2) {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > ctx.pages.lpage_size() {
    // Cannot write greater than one tile size in one request.
    return Err(OpError::InexactWriteLength);
  };

  // Read fields before `key` i.e. `size`, `obj_id`, `key_len`.
  let raw = ctx.device.read_at(inode_dev_offset, INODE_OFF.key()).await;
  let object_id = raw.read_u64_be_at(INODE_OFF.object_id());
  let size = raw.read_u40_be_at(INODE_OFF.size());
  let key_len = raw.read_u16_be_at(INODE_OFF.key_len());
  trace!(
    object_id = req.object_id,
    inode_dev_offset,
    size,
    key = key_debug_str(&ctx.device.read_at(INODE_OFF.key(), key_len.into()).await),
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

  if req.offset + len != min(req.offset + ctx.pages.lpage_size(), size) {
    // Write does not fully fill lpage segment or tail. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

  let InodeLayout {
    lpage_segment_count,
    tail_segment_page_sizes_pow2,
  } = calc_inode_layout(&ctx.pages, size);
  let off = INODE_OFF
    .with_key_len(key_len)
    .with_lpage_segments(lpage_segment_count)
    .with_tail_segments(tail_segment_page_sizes_pow2.len());
  // Vec of (page_size, page_dev_offset).
  let write_dev_offsets = {
    let idx = div_pow2(req.offset, ctx.pages.lpage_size_pow2);
    if idx < lpage_segment_count {
      vec![(
        ctx.pages.lpage_size(),
        Off64AsyncReadInt::read_u48_be_at(&ctx.device, off.lpage_segment(idx)).await,
      )]
    } else {
      let raw = ctx
        .device
        .read_at(
          off.tail_segments(),
          6 * u64!(tail_segment_page_sizes_pow2.len()),
        )
        .await;
      tail_segment_page_sizes_pow2
        .into_iter()
        .map(|(i, sz)| (1 << sz, raw.read_u48_be_at(u64!(i) * 6)))
        .collect_vec()
    }
  };

  let mut written = 0;
  let mut write_page_idx = 0;
  let mut buf = Vec::new();
  loop {
    // Incomplete object may have expired while we were writing. This is important to check regularly as we must not write after an object has been released, which would otherwise cause corruption.
    if incomplete_object_has_expired() {
      return Err(OpError::ObjectNotFound);
    };
    let Ok(maybe_chunk) = timeout(Duration::from_secs(60), req.data_stream.next()).await else {
      // We timed out.
      continue;
    };
    let Some(chunk) = maybe_chunk else {
      // Stream has ended.
      break;
    };
    buf.append(&mut chunk.map_err(|err| OpError::DataStreamError(Box::from(err)))?);
    let buf_len = u64!(buf.len());
    if written + buf_len > len {
      return Err(OpError::DataStreamLengthMismatch);
    };
    let (tgt_page_size, tgt_page_dev_offset) = write_dev_offsets[write_page_idx];
    if buf_len < tgt_page_size {
      continue;
    };
    // Optimisation: fdatasync at end of all writes instead of here.
    ctx
      .device
      .write_at(
        tgt_page_dev_offset,
        buf.drain(..usz!(tgt_page_size)).collect_vec(),
      )
      .await;
    trace!(object_id, written, buf_len, "wrote chunk");
    written += buf_len;
    write_page_idx += 1;
  }
  if written != len {
    return Err(OpError::DataStreamLengthMismatch);
  };

  // Optimisation: perform fdatasync in batches.
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  Ok(OpWriteObjectOutput {})
}
