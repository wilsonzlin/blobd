use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::layout::calc_object_layout;
use crate::object::offset::ObjectMetadataOffsets;
use crate::object::ObjectState;
use crate::state::action::create_object::ActionCreateObjectInput;
use crate::state::StateAction;
use crate::util::get_now_sec;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::u8;
use off64::usz;
use off64::Off64WriteMut;
use signal_future::SignalFuture;
use std::cmp::max;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub struct OpCreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
  pub assoc_data: TinyBuf,
}

pub struct OpCreateObjectOutput {
  pub token: IncompleteToken,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  let key_len = u16!(req.key.len());

  let layout = calc_object_layout(&ctx.pages, req.size);

  let offsets = ObjectMetadataOffsets {
    key_len,
    lpage_count: layout.lpage_count,
    tail_page_count: layout.tail_page_sizes_pow2.len(),
    assoc_data_len: u16!(req.assoc_data.len()),
  };

  let meta_size = offsets._total_size();
  if meta_size > ctx.pages.lpage_size() {
    return Err(OpError::ObjectMetadataTooLarge);
  };

  let created_sec = get_now_sec();

  let object_id = ctx.object_id_serial.next();

  let metadata_page_size = max(ctx.pages.spage_size(), meta_size.next_power_of_two());
  // This is stored in memory, so don't allocate rounded up to spage if less, as that's a significant waste of memory.
  // When the action writes, it'll copy the data to the page size.
  let mut raw = ctx.pages.allocate_uninitialised(meta_size);
  raw[usz!(offsets.page_size_pow2())] = u8!(metadata_page_size.ilog2());
  raw[usz!(offsets.state())] = ObjectState::Incomplete as u8;
  raw.write_u64_le_at(offsets.id(), object_id);
  raw.write_u40_le_at(offsets.size(), req.size);
  raw.write_u48_le_at(offsets.created_sec(), created_sec);
  raw.write_u16_le_at(offsets.key_len(), key_len);
  raw.write_at(offsets.key(), &req.key);
  raw.write_u16_le_at(offsets.assoc_data_len(), u16!(req.assoc_data.len()));
  raw.write_at(offsets.assoc_data(), &req.assoc_data);

  let (fut, fut_ctl) = SignalFuture::new();
  ctx.state.send_action(StateAction::Create(
    ActionCreateObjectInput {
      metadata_page_size,
      layout,
      offsets,
      raw,
    },
    fut_ctl,
  ));
  fut.await
}
