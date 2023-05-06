use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::ObjectState;
use crate::util::div_pow2;
use crate::util::is_multiple_of_pow2;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use off64::u32;
use off64::u64;
use off64::u8;
use off64::usz;
use std::cmp::max;
use std::cmp::min;
use std::error::Error;
use std::iter::empty;
use std::sync::Arc;
use tracing::trace;
use tracing::warn;

pub struct OpWriteObjectInput<
  D: AsRef<[u8]>,
  S: Unpin + Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
> {
  pub incomplete_token: IncompleteToken,
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
  let IncompleteToken { object_id, .. } = req.incomplete_token;

  let Some(obj) = ctx.incomplete_objects.read().get(&object_id).cloned() else {
    return Err(OpError::ObjectNotFound);
  };

  if !is_multiple_of_pow2(req.offset, ctx.pages.lpage_size_pow2) {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > ctx.pages.lpage_size() {
    // Cannot write greater than one lpage size in one request.
    return Err(OpError::InexactWriteLength);
  };

  if req.offset + len > obj.size() {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len != min(req.offset + ctx.pages.lpage_size(), obj.size()) {
    // Write does not fully fill lpage or entire tail. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

  // Vec of (page_size, page_dev_offset).
  let write_dev_offsets = {
    let idx = u32!(div_pow2(req.offset, ctx.pages.lpage_size_pow2));
    if idx < obj.lpage_count() {
      vec![(ctx.pages.lpage_size(), obj.lpage_dev_offset(idx))]
    } else {
      let mut offsets = obj
        .tail_page_sizes()
        .into_iter()
        .map(|(i, sz)| (1 << sz, obj.tail_page_dev_offset(u8!(i))))
        .collect_vec();
      // For the very last tail page, we don't write a full page amount of bytes, unless the object just happens to be a multiple of that page's size. Use `.map` as there may not even be any tail pages at all.
      offsets.last_mut().map(|(page_size, _page_dev_offset)| {
        let mod_ = obj.size() % *page_size;
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
    let maybe_chunk = req.data_stream.next().await;
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

    // TODO We could write more frequently instead of buffering an entire page if the page is larger than one SSD page/block write. However, if it's smaller, the I/O system (e.g. mmap) would be doing buffering and repeated writes anyway.
    let (amount_to_write, page_dev_offset) = write_dev_offsets[write_page_idx];
    if buf_len < amount_to_write {
      continue;
    };
    {
      let lock = obj
        .lock_for_writing_if_still_valid(ObjectState::Incomplete)
        .await?;
      // Optimisation: fdatasync at end of all writes instead of here.
      // We cann't use `allocate_from_data` as it won't be sized correctly.
      let mut write_data = ctx.pages.allocate_uninitialised(max(
        ctx.pages.spage_size(),
        amount_to_write.next_power_of_two(),
      ));
      write_data[..usz!(amount_to_write)].copy_from_slice(&buf[..usz!(amount_to_write)]);
      buf.splice(..usz!(amount_to_write), empty());
      ctx.device.write_at(page_dev_offset, write_data).await;
      trace!(
        object_id,
        previously_written = written,
        page_write_amount = amount_to_write,
        "wrote page"
      );
      written += amount_to_write;
      write_page_idx += 1;
      drop(lock);
    };
  }
  if written != len {
    warn!(
      received = written,
      declared = len,
      "stream provided fewer data than declared"
    );
    return Err(OpError::DataStreamLengthMismatch);
  };

  ctx.device.sync().await;

  Ok(OpWriteObjectOutput {})
}
