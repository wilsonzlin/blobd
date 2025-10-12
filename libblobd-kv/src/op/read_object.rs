use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::object::ObjectTupleData;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use bufpool::buf::Buf;
use off64::u64;
use off64::usz;
use std::cmp::max;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub struct OpReadObjectInput {
  pub key: Vec<u8>,
  pub start: u64,
  // Exclusive.
  pub end: Option<u64>,
}

pub struct OpReadObjectOutput {
  pub data: Vec<u8>,
  pub end: u64,
  pub object_size: u64,
}

/// Both `offset` and `len` do not have to be multiples of the spage size.
async fn unaligned_read(ctx: &Ctx, offset: u64, len: u64) -> Buf {
  let a_start = floor_pow2(offset, ctx.pages.spage_size_pow2);
  let a_end = max(
    ceil_pow2(offset + len, ctx.pages.spage_size_pow2),
    ctx.pages.spage_size(),
  );
  let mut buf = ctx.device.read_at(a_start, a_end - a_start).await;
  ctx
    .metrics
    .0
    .read_op_bytes_discarded
    .fetch_add((a_end - a_start) - len, Relaxed);
  buf.copy_within(usz!(offset - a_start)..usz!(offset - a_start + len), 0);
  buf.truncate(usz!(len));
  buf
}

pub(crate) async fn op_read_object(
  ctx: Arc<Ctx>,
  req: OpReadObjectInput,
) -> OpResult<OpReadObjectOutput> {
  let start = req.start;

  let res = match ctx
    .log_buffer
    .read_tuple(req.key)
    .await
    .ok_or_else(|| OpError::ObjectNotFound)?
  {
    ObjectTupleData::Inline(i) => {
      let object_size = u64!(i.len());
      let end = req.end.unwrap_or(object_size);
      if start > end || start > object_size || end > object_size {
        Err(OpError::RangeOutOfBounds)
      } else {
        Ok(OpReadObjectOutput {
          data: i[usz!(start)..usz!(end)].to_vec(),
          end,
          object_size,
        })
      }
    }
    ObjectTupleData::Heap {
      size: object_size,
      dev_offset,
    } => {
      let object_size = u64!(object_size);
      let end = req.end.unwrap_or(object_size);
      if start > end || start > object_size || end > object_size {
        Err(OpError::RangeOutOfBounds)
      } else {
        Ok(OpReadObjectOutput {
          data: if start == end || start == object_size {
            Vec::new()
          } else {
            unaligned_read(&ctx, dev_offset + start, end - start)
              .await
              .to_vec()
          },
          end,
          object_size,
        })
      }
    }
  };

  if let Ok(res) = res.as_ref() {
    ctx.metrics.0.read_op_count.fetch_add(1, Relaxed);
    ctx
      .metrics
      .0
      .read_op_bytes_sent
      .fetch_add(u64!(res.data.len()), Relaxed);
  };

  res
}
