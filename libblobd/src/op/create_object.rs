use super::OpResult;
use crate::ctx::Ctx;
use crate::object::calc_object_layout;
use crate::object::ObjectLayout;
use crate::object::OBJECT_OFF;
use crate::op::key_debug_str;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::usz;
use off64::Off64WriteMut;
use std::sync::Arc;
use tracing::trace;

pub struct OpCreateObjectInput {
  pub key: Vec<u8>,
  pub size: u64,
  pub assoc_data: Vec<u8>,
}

pub struct OpCreateObjectOutput {
  pub inode_dev_offset: u64,
  pub object_id: u64,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len = u16!(req.key.len());
  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(&ctx.pages, req.size);

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len())
    .with_assoc_data_len(u16!(req.assoc_data.len()));
  let meta_size = off._total_size();
  if meta_size > ctx.pages.lpage_size() {
    return Err(OpError::ObjectMetadataTooLarge);
  };

  trace!(
    key = key_debug_str(&req.key),
    meta_size,
    size = req.size,
    lpage_count,
    tail_page_count = tail_page_sizes_pow2.len(),
    "creating object"
  );

  let mut raw = vec![0u8; usz!(meta_size)];
  raw.write_u40_be_at(off.size(), req.size);
  raw.write_u16_be_at(off.key_len(), key_len);
  raw.write_at(off.key(), &req.key);
  raw.write_u16_be_at(off.assoc_data_len(), u16!(req.assoc_data.len()));
  raw.write_at(off.assoc_data(), &req.assoc_data);

  let (txn, dev_offset, object_id) = {
    let mut state = ctx.state.lock().await;
    let mut txn = ctx.journal.begin_transaction();

    let object_id = state.object_id_serial.next(&mut txn);
    raw.write_u64_be_at(off.id(), object_id);

    // TODO Parallelise all awaits and loops.
    trace!(key = key_debug_str(&req.key), "allocating metadata for object");
    let dev_offset = state.allocator.allocate(&mut txn, meta_size).await;
    for i in 0..lpage_count {
      trace!(
        key = key_debug_str(&req.key),
        lpage_index = i,
        "allocating lpage for object"
      );
      let lpage_dev_offset = state
        .allocator
        .allocate(&mut txn, ctx.pages.lpage_size())
        .await;
      raw.write_u48_be_at(off.lpage(i), lpage_dev_offset);
    }
    for (i, tail_page_size_pow2) in tail_page_sizes_pow2 {
      trace!(
        key = key_debug_str(&req.key),
        tail_page_size_pow2,
        "allocating tail page for object"
      );
      let page_dev_offset = state
        .allocator
        .allocate(&mut txn, 1 << tail_page_size_pow2)
        .await;
      raw.write_u48_be_at(off.tail_page(i), page_dev_offset);
    }

    state
      .incomplete_list
      .attach(&mut txn, dev_offset)
      .await;

    (txn, dev_offset, object_id)
  };

  trace!(
    key = key_debug_str(&req.key),
    object_id,
    dev_offset,
    "allocated object"
  );

  ctx.device.write_at(dev_offset, raw).await;
  ctx.journal.commit_transaction(txn).await;
  trace!(key = key_debug_str(&req.key), object_id, "created object");

  Ok(OpCreateObjectOutput {
    inode_dev_offset,
    object_id,
  })
}
