use super::OpError;
use super::OpResult;
use super::UnalignedReader;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::layout::calc_object_layout;
use crate::object::layout::ObjectLayout;
use crate::object::offset::OBJECT_OFF;
use crate::object::ObjectState;
use crate::util::div_pow2;
use crate::util::is_multiple_of_pow2;
use bufpool::BUFPOOL;
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
  let IncompleteToken {
    object_id, key_len, ..
  } = req.incomplete_token;

  let Some(obj_strong) = ctx.incomplete_list.get(object_id) else {
    return Err(OpError::ObjectNotFound);
  };
  let object_id = obj_strong.id;
  let object_dev_offset = obj_strong.dev_offset;
  let object_size = obj_strong.size;
  // We only need to check the state sometimes, so let the object drop in between those times; otherwise, we'll block all commits in the serial state worker loop if it does get deleted.
  let obj = obj_strong.into_weak();

  if !is_multiple_of_pow2(req.offset, ctx.lpage_size_pow2) {
    // Invalid offset.
    return Err(OpError::UnalignedWrite);
  };
  if len > (1 << ctx.lpage_size_pow2) {
    // Cannot write greater than one tile size in one request.
    return Err(OpError::InexactWriteLength);
  };

  if req.offset + len > object_size {
    // Offset is past size.
    return Err(OpError::RangeOutOfBounds);
  };

  if req.offset + len != min(req.offset + (1 << ctx.lpage_size_pow2), object_size) {
    // Write does not fully fill lpage or entire tail. All writes must fill as otherwise uninitialised data will get exposed.
    return Err(OpError::InexactWriteLength);
  };

  let unaligned_reader = UnalignedReader::new(ctx.device.clone(), ctx.spage_size_pow2);

  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(ctx.spage_size_pow2, ctx.lpage_size_pow2, object_size);
  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len());
  // Vec of (page_size, page_dev_offset).
  let write_dev_offsets = {
    let idx = div_pow2(req.offset, ctx.lpage_size_pow2);
    if idx < lpage_count {
      vec![(
        (1 << ctx.lpage_size_pow2),
        unaligned_reader
          .read_u48_be_at(object_dev_offset + off.lpage(idx))
          .await,
      )]
    } else {
      let raw = unaligned_reader
        .read(
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
        let mod_ = object_size % *page_size;
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
      let _lock = obj.lock_if_still_valid(ObjectState::Incomplete)?;
      // Optimisation: fdatasync at end of all writes instead of here.
      let mut write_data = BUFPOOL.allocate(usz!(amount_to_write));
      write_data.extend(buf.drain(..usz!(amount_to_write)));
      ctx.device.write(page_dev_offset, write_data).await;
      trace!(
        object_id,
        previously_written = written,
        page_write_amount = amount_to_write,
        "wrote page"
      );
      written += amount_to_write;
      write_page_idx += 1;
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
