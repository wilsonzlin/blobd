use crate::incomplete_token::IncompleteToken;
use crate::journal::Transaction;
use crate::object::layout::calc_object_layout;
use crate::object::layout::ObjectLayout;
use crate::object::offset::OBJECT_OFF;
use crate::object::ObjectMetadata;
use crate::op::create_object::OpCreateObjectOutput;
use crate::op::OpError;
use crate::op::OpResult;
use crate::state::State;
use crate::util::get_now_sec;
use bufpool::BUFPOOL;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::usz;
use off64::Off64WriteMut;
use tinybuf::TinyBuf;

pub(crate) struct ActionCreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
  pub assoc_data: TinyBuf,
}

pub(crate) fn action_create_object(
  state: &mut State,
  txn: &mut Transaction,
  req: ActionCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len = u16!(req.key.len());

  let ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  } = calc_object_layout(
    state.cfg.spage_size_pow2,
    state.cfg.lpage_size_pow2,
    req.size,
  );

  let off = OBJECT_OFF
    .with_key_len(key_len)
    .with_lpages(lpage_count)
    .with_tail_pages(tail_page_sizes_pow2.len())
    .with_assoc_data_len(u16!(req.assoc_data.len()));

  let meta_size = off._total_size();
  if meta_size > state.cfg.lpage_size() {
    return Err(OpError::ObjectMetadataTooLarge);
  };

  let created_sec = get_now_sec();

  let object_id = state.object_id_serial.next();

  let dev_offset = state.allocator.allocate(meta_size);

  let mut raw = BUFPOOL.allocate_with_zeros(usz!(meta_size));
  raw.write_u16_be_at(off.key_len(), key_len);
  raw.write_at(off.key(), &req.key);
  raw.write_u16_be_at(off.assoc_data_len(), u16!(req.assoc_data.len()));
  raw.write_at(off.assoc_data(), &req.assoc_data);

  for i in 0..lpage_count {
    let lpage_dev_offset = state.allocator.allocate(state.cfg.lpage_size());
    raw.write_u48_be_at(off.lpage(i), lpage_dev_offset);
  }
  for (i, tail_page_size_pow2) in tail_page_sizes_pow2 {
    let page_dev_offset = state.allocator.allocate(1 << tail_page_size_pow2);
    raw.write_u48_be_at(off.tail_page(i), page_dev_offset);
  }

  state.incomplete_list.insert(ObjectMetadata {
    created_sec,
    dev_offset,
    id: object_id,
    size: req.size,
  });

  state.metrics.incr_object_count(1);
  state.metrics.incr_object_data_bytes(req.size);
  state.metrics.incr_object_metadata_bytes(meta_size);

  txn.record(dev_offset, raw);

  Ok(OpCreateObjectOutput {
    token: IncompleteToken {
      bucket_id: state.buckets.bucket_id_for_key(&req.key),
      object_id,
      key_len,
    },
  })
}
