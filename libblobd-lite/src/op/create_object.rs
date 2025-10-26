use super::OpResult;
use crate::allocator::layout::ObjectLayout;
use crate::allocator::layout::calc_object_layout;
use crate::ctx::Ctx;
use crate::object::OBJECT_OFF;
use crate::object::ObjectState;
use crate::op::OpError;
use crate::op::key_debug_str;
use crate::util::get_now_ms;
use off64::Off64WriteMut;
use off64::int::Off64WriteMutInt;
use off64::int::create_u48_be;
use off64::u16;
use off64::usz;
use rand::Rng;
use rand::thread_rng;
use std::cmp::max;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use tracing::instrument;
use tracing::trace;

pub struct OpCreateObjectInput {
  pub key: Vec<u8>,
  pub size: u64,
  pub assoc_data: Vec<u8>,
}

pub struct OpCreateObjectOutput {
  pub object_id: u128,
}

#[instrument(skip_all)]
pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len = u16!(req.key.len());
  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(ctx.pages, req.size);

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len())
    .with_assoc_data_len(u16!(req.assoc_data.len()));
  let meta_size = off._total_size();
  if meta_size > ctx.pages.lpage_size() {
    return Err(OpError::ObjectMetadataTooLarge);
  };
  let meta_size_pow2 = max(
    ctx.pages.spage_size_pow2,
    meta_size.next_power_of_two().ilog2().try_into().unwrap(),
  );
  assert!(meta_size_pow2 <= ctx.pages.lpage_size_pow2);

  trace!(
    key = key_debug_str(&req.key),
    meta_size,
    size = req.size,
    lpage_count,
    tail_page_count = tail_page_sizes_pow2.len(),
    "creating object"
  );

  let object_id: u128 = thread_rng().r#gen();
  let created_ms = get_now_ms();

  let mut raw = vec![0u8; usz!(meta_size)];
  raw.write_u8_at(off.state(), ObjectState::Incomplete as u8);
  raw.write_u8_at(off.metadata_size_pow2(), meta_size_pow2);
  raw.write_u128_be_at(off.id(), object_id);
  raw.write_u48_be_at(off.created_ms(), created_ms);
  raw.write_u40_be_at(off.size(), req.size);
  raw.write_u16_be_at(off.key_len(), key_len);
  raw.write_at(off.key(), &req.key);
  raw.write_u16_be_at(off.assoc_data_len(), u16!(req.assoc_data.len()));
  raw.write_at(off.assoc_data(), &req.assoc_data);

  let dev_offset = {
    let mut allocator = ctx.allocator.lock();
    for i in 0..lpage_count {
      let lpage_dev_offset = allocator.allocate(ctx.pages.lpage_size()).unwrap();
      raw.write_u48_be_at(off.lpage(i), lpage_dev_offset);
    }
    for (i, tail_page_size_pow2) in tail_page_sizes_pow2 {
      let page_dev_offset = allocator.allocate(1 << tail_page_size_pow2).unwrap();
      raw.write_u48_be_at(off.tail_page(i), page_dev_offset);
    }
    allocator.allocate(meta_size).unwrap()
  };

  // Write before updating bucket list, as as soon as overlay is updated, the object must be reachable.
  ctx.device.write_at(dev_offset, &raw).await;

  let (txn, bkt_overlay_entry, next_overlay_entry) = {
    let locker = ctx.buckets.get_locker_for_key(&req.key);
    let mut bkt = locker.write().await;
    let mut txn = bkt.begin_transaction();

    // Get the current bucket head.
    // SAFETY: This must use the overlay (otherwise stale read), and only be done after acquiring lock (otherwise TOCTOU).
    let cur_bkt_head = bkt.get_head().await;

    // Update bucket head to point to this new inode.
    let bkt_overlay_entry = bkt.update_head(&mut txn, dev_offset);

    // Set inode next pointer.
    let next_overlay_entry = ctx.overlay.set_object_next(object_id, cur_bkt_head);
    txn.write(
      dev_offset + off.next_node_dev_offset(),
      create_u48_be(cur_bkt_head).to_vec(),
    );

    (ctx.buckets.commit_transaction(txn), bkt_overlay_entry, next_overlay_entry)
  };
  if let Some(signal) = txn {
    signal.await;
  }
  ctx.overlay.evict(bkt_overlay_entry);
  ctx.overlay.evict(next_overlay_entry);
  trace!(key = key_debug_str(&req.key), object_id, "created object");

  ctx.metrics.object_count.fetch_add(1, Relaxed);
  ctx.metrics.object_data_bytes.fetch_add(req.size, Relaxed);
  ctx
    .metrics
    .object_metadata_bytes
    .fetch_add(meta_size, Relaxed);

  Ok(OpCreateObjectOutput { object_id })
}
