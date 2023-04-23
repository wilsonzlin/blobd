use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::calc_object_layout;
use crate::object::ObjectLayout;
use crate::object::OBJECT_OFF;
use crate::op::key_debug_str;
use crate::op::OpError;
use crate::util::get_now_ms;
use off64::int::Off64AsyncWriteInt;
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
  pub token: IncompleteToken,
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

  let created_ms = get_now_ms();

  let mut raw = vec![0u8; usz!(meta_size)];
  raw.write_u48_be_at(off.created_ms(), created_ms);
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

    trace!(
      key = key_debug_str(&req.key),
      "allocating metadata for object"
    );
    let (dev_offset, meta_size_pow2) = state
      .allocator
      .allocate_and_ret_with_size(&mut txn, meta_size)
      .await;

    // This is a one-time special direct write to prevent an edge case where the object is attached to the incomplete list, and the incomplete list tries to read the object's metadata (e.g. creation time), *before* the journal has actually written it out to the mmap. Normally this is dangerous because an unexpected page writeback could leave the device in a corrupted state, but in this specific case it's fine because we're writing to free space, so even if we crash right after this it's just junk. This specific issue can only occur here because object metadata is immutable and all other ops on the object can only occur after the journal has committed and the reseponse has been returned.
    ctx
      .device
      .write_u48_be_at(dev_offset + off.created_ms(), created_ms)
      .await;

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
      .attach(&mut txn, dev_offset, meta_size_pow2)
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
    token: IncompleteToken {
      created_sec: created_ms / 1000,
      object_dev_offset: dev_offset,
    },
  })
}
