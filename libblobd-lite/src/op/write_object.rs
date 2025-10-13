use super::OpError;
use super::OpResult;
use crate::bucket::FoundObject;
use crate::ctx::Ctx;
use crate::object::OBJECT_OFF;
use crate::object::ObjectState;
use crate::op::key_debug_str;
use crate::util::div_pow2;
use crate::util::is_multiple_of_pow2;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64ReadInt;
use off64::u16;
use off64::u64;
use off64::usz;
use std::cmp::min;
use std::error::Error;
use std::sync::Arc;
use tracing::trace;
use tracing::warn;

pub struct OpWriteObjectInput<
  D: AsRef<[u8]>,
  S: Unpin + Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
> {
  pub key: Vec<u8>,
  pub object_id: u128,
  pub offset: u64,
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
  let object_id = req.object_id;
  let key_len = u16!(req.key.len());
  trace!(
    key = key_debug_str(&req.key),
    object_id,
    offset = req.offset,
    length = req.data_len,
    "writing object"
  );

  let locker = ctx.buckets.get_locker_for_key(&req.key);
  let bkt = locker.read().await;

  let Some(FoundObject {
    dev_offset: object_dev_offset,
    meta,
    ..
  }) = bkt
    .find_object(ObjectState::Committed, Some(object_id))
    .await
  else {
    return Err(OpError::ObjectNotFound);
  };
  let size = meta.size();
  let lpage_count = meta.lpage_count();
  let tail_page_sizes_pow2 = meta.tail_page_sizes_pow2();

  if !is_multiple_of_pow2(req.offset, ctx.pages.lpage_size_pow2) {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > ctx.pages.lpage_size() {
    // Cannot write greater than one tile size in one request.
    return Err(OpError::InexactWriteLength);
  };
  trace!(
    object_id,
    object_dev_offset, size, "found object to write to"
  );

  if req.offset + len > size {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len != min(req.offset + ctx.pages.lpage_size(), size) {
    // Write does not fully fill lpage or entire tail. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

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
        Off64AsyncReadInt::read_u48_be_at(ctx.device.as_ref(), object_dev_offset + off.lpage(idx))
          .await,
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
    if let Some(chunk) = req.data_stream.next().await {
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
        &buf.drain(..usz!(amount_to_write)).collect_vec(),
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
  ctx.device.write_at_with_delayed_sync(vec![]).await;

  Ok(OpWriteObjectOutput {})
}
