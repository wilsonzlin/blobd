use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::calc_object_layout;
use crate::object::ObjectLayout;
use crate::object::OBJECT_OFF;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
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
use tracing::trace;
use tracing::warn;

pub struct OpWriteObjectInput<
  D: AsRef<[u8]>,
  S: Unpin + Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
> {
  pub offset: u64,
  pub incomplete_token: IncompleteToken,
  pub data_len: u64,
  pub data_stream: S,
}

pub struct OpWriteObjectOutput {}

pub(crate) async fn op_write_object<
  D: AsRef<[u8]>,
  S: Unpin + Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
>(
  ctx: Arc<Ctx>,
  mut req: OpWriteObjectInput<D, S>,
) -> OpResult<OpWriteObjectOutput> {
  let len = req.data_len;
  let object_dev_offset = req.incomplete_token.object_dev_offset;
  trace!(
    dev_offset = object_dev_offset,
    offset = req.offset,
    length = req.data_len,
    "writing object"
  );

  // See IncompleteToken for why if the token has not expired, the object definitely still exists (i.e. safe to read any metadata).
  if req
    .incomplete_token
    .has_expired(ctx.reap_objects_after_secs)
  {
    return Err(OpError::ObjectNotFound);
  };

  let incomplete_object_is_still_valid = || async {
    // Our incomplete reaper simply deletes incomplete objects instead of reaping directly, which avoids some clock drift issues, so we only need to check the type, and should not check if it's expired based on its creation time. This is always correct, as if the page still exists, it's definitely still the same object, as we check well before any deleted object would be reaped.
    let hdr = ctx
      .pages
      .read_page_header::<ObjectPageHeader>(object_dev_offset)
      .await;
    hdr.state == ObjectState::Incomplete
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
  let raw = ctx
    .device
    .read_at(object_dev_offset, OBJECT_OFF.key())
    .await;
  let object_id = raw.read_u64_be_at(OBJECT_OFF.id());
  let size = raw.read_u40_be_at(OBJECT_OFF.size());
  let key_len = raw.read_u16_be_at(OBJECT_OFF.key_len());
  trace!(
    object_id,
    object_dev_offset,
    size,
    "found object to write to"
  );

  if req.offset + len > size {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len != min(req.offset + ctx.pages.lpage_size(), size) {
    // Write does not fully fill lpage or entire tail. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(&ctx.pages, size);
  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len());
  // Vec of (page_size, page_dev_offset).
  let write_dev_offsets = {
    let idx = div_pow2(req.offset, ctx.pages.lpage_size_pow2);
    if idx < lpage_count {
      vec![(
        ctx.pages.lpage_size(),
        Off64AsyncReadInt::read_u48_be_at(&ctx.device, object_dev_offset + off.lpage(idx)).await,
      )]
    } else {
      let raw = ctx
        .device
        .read_at(
          object_dev_offset + off.tail_pages(),
          6 * u64!(tail_page_sizes_pow2.len()),
        )
        .await;
      let mut offsets = tail_page_sizes_pow2
        .into_iter()
        .map(|(i, sz)| (1 << sz, raw.read_u48_be_at(u64!(i) * 6)))
        .collect_vec();
      // For the very last tail page, we don't write a full page amount of bytes, unless the object just happens to be a multiple of that page's size. Use `.map` as there may not even be any tail pages at all.
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
  loop {
    // See comment for code below.
    if !incomplete_object_is_still_valid().await {
      return Err(OpError::ObjectNotFound);
    };
    let Ok(maybe_chunk) = timeout(Duration::from_secs(60), req.data_stream.next()).await else {
      // We timed out, and need to check if the object is still valid.
      continue;
    };
    if let Some(chunk) = maybe_chunk {
      buf.extend_from_slice(
        chunk
          .map_err(|err| OpError::DataStreamError(Box::from(err)))?
          .as_ref(),
      );
    } else if buf.is_empty() {
      // Stream has ended and buffer has been fully consumed.
      break;
    };
    let buf_len = u64!(buf.len());
    if written + buf_len > len {
      warn!(
        received = written + buf_len,
        declared = len,
        "stream provided more data than declared"
      );
      return Err(OpError::DataStreamLengthMismatch);
    };

    // We have two reasons to check the object state again:
    // - Prevent use-after-free: incomplete object may have expired while we were writing. This is important to check regularly as we must not write after an object has been released, which would otherwise cause corruption. We need to do this well before actual reap time, to account for possible clock drift and slow execution delaying checks, but no need to do every iteration, e.g. around every 60 seconds.
    // - Prevent writing after committing: unlike use-after-free, this doesn't actually lead to any corruption, but it's to assist the user to ensure that what they get after they commit is always the same, very useful when creator is different from reader (e.g. content is uploaded by customer, and then hashed and processed by service straight away). AFAICT, doing this every iteration just before writing should be good enough, only possibly microseconds delay due to CPU cache coherence as we're not using locks, atomics, or memory barriers to read the object state. This should be reasonably fast given the object metadata should be in the page cache.
    if !incomplete_object_is_still_valid().await {
      return Err(OpError::ObjectNotFound);
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
  #[cfg(not(test))]
  ctx
    .device
    .write_at_with_delayed_sync::<&'static [u8]>(vec![])
    .await;

  Ok(OpWriteObjectOutput {})
}
