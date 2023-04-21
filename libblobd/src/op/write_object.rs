use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::inode::calc_inode_layout;
use crate::inode::InodeLayout;
use crate::inode::INODE_OFF;
use crate::page::PageType;
use crate::util::div_pow2;
use crate::util::is_multiple_of_pow2;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64ReadInt;
use off64::u64;
use off64::usz;
use std::cmp::min;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio::time::Instant;
use tracing::trace;
use tracing::warn;

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

  let incomplete_object_is_still_valid = || async {
    // Our incomplete reaper simply deletes incomplete objects instead of reaping directly, which avoids some clock drift issues, so we only need to check the type, and should not check if it's expired based on its creation time. This is always correct, as if the page still exists, it's definitely still the same object, as we check well before any deleted object would be reaped.
    let (hdr_type, _) = ctx
      .pages
      .read_page_header_type_and_size(inode_dev_offset)
      .await;
    // TODO It's a user bug if they've committed before they've finished all writes (including ones that haven't started yet), but should we assert that it's not PageType::ActiveInode or PageType::DeletedInode just to be safe?
    hdr_type == PageType::IncompleteInode
  };

  if !incomplete_object_is_still_valid().await {
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
      let mut offsets = tail_segment_page_sizes_pow2
        .into_iter()
        .map(|(i, sz)| (1 << sz, raw.read_u48_be_at(u64!(i) * 6)))
        .collect_vec();
      // For the very last tail page, we don't write a full page amount of bytes, unless the object just happens to be a multiple of that page's size. Use `.map` as there may not even be any tail segments at all.
      offsets.last_mut().map(|(page_size, _page_dev_offset)| {
        let mod_ = size % *page_size;
        if mod_ != 0 {
          *page_size = mod_;
        };
      });
      offsets
    }
  };

  let mut written = 0;
  let mut write_page_idx = 0;
  let mut buf = Vec::new();
  let mut last_checked_valid = Instant::now();
  loop {
    // Incomplete object may have expired while we were writing. This is important to check regularly as we must not write after an object has been released, which would otherwise cause corruption.
    let now = Instant::now();
    if now.duration_since(last_checked_valid).as_secs() >= 60 {
      if !incomplete_object_is_still_valid().await {
        return Err(OpError::ObjectNotFound);
      };
      last_checked_valid = now;
    };
    let Ok(maybe_chunk) = timeout(Duration::from_secs(60), req.data_stream.next()).await else {
      // We timed out, and need to check if the object is still valid.
      continue;
    };
    let Some(chunk) = maybe_chunk else {
      // Stream has ended.
      break;
    };
    buf.append(&mut chunk.map_err(|err| OpError::DataStreamError(Box::from(err)))?);
    let buf_len = u64!(buf.len());
    if written + buf_len > len {
      warn!(
        received = written + buf_len,
        declared = len,
        "stream provided more data than declared"
      );
      return Err(OpError::DataStreamLengthMismatch);
    };
    // TODO We could write more frequently instead of buffering an entire page if the page is larger than one SSD page/block write. However, if it's smaller, the I/O system (e.g. mmap) would be doing buffering and repeated writes anyway.
    let (amount_to_write, page_dev_offset) = write_dev_offsets[write_page_idx];
    if buf_len < amount_to_write {
      continue;
    };
    // Optimisation: fdatasync at end of all writes instead of here.
    ctx
      .device
      .write_at(
        page_dev_offset,
        buf.drain(..usz!(amount_to_write)).collect_vec(),
      )
      .await;
    trace!(
      object_id,
      previously_written = written,
      page_write_amount = amount_to_write,
      "wrote page"
    );
    written += amount_to_write;
    write_page_idx += 1;
  }
  if written != len {
    warn!(
      received = written,
      declared = len,
      "stream provided fewer data than declared"
    );
    return Err(OpError::DataStreamLengthMismatch);
  };

  // Optimisation: perform fdatasync in batches.
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  Ok(OpWriteObjectOutput {})
}
