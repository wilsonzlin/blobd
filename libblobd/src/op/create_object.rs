use super::OpResult;
use crate::ctx::Ctx;
use crate::object::calc_object_layout;
use crate::object::ObjectLayout;
use crate::object::OBJECT_OFF;
use crate::op::key_debug_str;
use off64::int::create_u16_be;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::usz;
use off64::Off64WriteMut;
use std::sync::Arc;
use tracing::trace;

pub struct OpCreateObjectInput {
  pub key: Vec<u8>,
  pub size: u64,
  pub custom_headers: Vec<(String, String)>,
}

pub struct OpCreateObjectOutput {
  pub inode_dev_offset: u64,
  pub object_id: u64,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len: u16 = req.key.len().try_into().unwrap();
  let ObjectLayout {
    lpage_segment_count,
    tail_segment_page_sizes_pow2,
  } = calc_object_layout(&ctx.pages, req.size);
  let custom_header_count = u16!(req.custom_headers.len());

  let mut custom_headers_raw = Vec::new();
  // TODO Validations?
  // - Name length.
  // - Value length.
  // - Lowercase.
  // - Duplicates.
  // - Characters that are not ASCII.
  // - Characters that are not display characters.
  // - Invalid characters.
  // - Whitespace in headers. (Could be malicious.)
  // - Whitelist of allowed overrides if not `x-`.
  for (name, value) in req.custom_headers {
    custom_headers_raw.extend_from_slice(&create_u16_be(u16!(name.len())));
    custom_headers_raw.extend_from_slice(&create_u16_be(u16!(value.len())));
    custom_headers_raw.extend_from_slice(name.as_bytes());
    custom_headers_raw.extend_from_slice(value.as_bytes());
  }

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpage_segments(lpage_segment_count)
    .with_tail_segments(tail_segment_page_sizes_pow2.len())
    .with_custom_headers(u16!(custom_headers_raw.len()));
  let inode_size = off._total_inode_size();
  // TODO
  if inode_size > ctx.pages.lpage_size() {
    panic!("inode too large");
  };

  trace!(
    key = key_debug_str(&req.key),
    inode_size,
    size = req.size,
    lpage_segment_count,
    tail_segment_count = tail_segment_page_sizes_pow2.len(),
    "creating object"
  );

  let mut inode_raw = vec![0u8; usz!(inode_size)];
  inode_raw.write_u40_be_at(off.size(), req.size);
  inode_raw.write_u16_be_at(off.key_len(), key_len);
  inode_raw.write_at(off.key(), &req.key);
  inode_raw.write_u16_be_at(
    off.custom_header_byte_count(),
    u16!(custom_headers_raw.len()),
  );
  inode_raw.write_u16_be_at(off.custom_header_entry_count(), custom_header_count);
  inode_raw.write_at(off.custom_headers(), &custom_headers_raw);

  let (txn, inode_dev_offset, object_id) = {
    let mut state = ctx.state.lock().await;
    let mut txn = ctx.journal.begin_transaction();

    let object_id = state.object_id_serial.next(&mut txn);
    inode_raw.write_u64_be_at(off.object_id(), object_id);

    // TODO Parallelise all awaits and loops.
    trace!(key = key_debug_str(&req.key), "allocating inode for object");
    let inode_dev_offset = state.allocator.allocate(&mut txn, inode_size).await;
    for i in 0..lpage_segment_count {
      trace!(
        key = key_debug_str(&req.key),
        lpage_segment_index = i,
        "allocating lpage for object"
      );
      let lpage_dev_offset = state
        .allocator
        .allocate(&mut txn, ctx.pages.lpage_size())
        .await;
      inode_raw.write_u48_be_at(off.lpage_segment(i), lpage_dev_offset);
    }
    for (i, tail_segment_page_size_pow2) in tail_segment_page_sizes_pow2 {
      trace!(
        key = key_debug_str(&req.key),
        tail_segment_page_size_pow2,
        "allocating tail page for object"
      );
      let page_dev_offset = state
        .allocator
        .allocate(&mut txn, 1 << tail_segment_page_size_pow2)
        .await;
      inode_raw.write_u48_be_at(off.tail_segment(i), page_dev_offset);
    }

    state
      .incomplete_list
      .attach(&mut txn, inode_dev_offset)
      .await;

    (txn, inode_dev_offset, object_id)
  };

  trace!(
    key = key_debug_str(&req.key),
    object_id,
    inode_dev_offset,
    "allocated object"
  );

  ctx.device.write_at(inode_dev_offset, inode_raw).await;
  ctx.journal.commit_transaction(txn).await;
  trace!(key = key_debug_str(&req.key), object_id, "created object");

  Ok(OpCreateObjectOutput {
    inode_dev_offset,
    object_id,
  })
}
