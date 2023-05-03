use crate::incomplete_token::IncompleteToken;
use crate::journal::Transaction;
use crate::object::layout::ObjectLayout;
use crate::object::offset::ObjectMetadataOffsets;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::op::create_object::OpCreateObjectOutput;
use crate::op::OpResult;
use crate::state::State;
use bufpool_fixed::buf::FixedBuf;
use off64::int::Off64WriteMutInt;
use off64::u64;

pub(crate) struct ActionCreateObjectInput {
  pub raw: FixedBuf,
  pub offsets: ObjectMetadataOffsets,
  pub layout: ObjectLayout,
}

pub(crate) fn action_create_object(
  state: &mut State,
  txn: &mut Transaction,
  ActionCreateObjectInput {
    mut raw,
    offsets,
    layout,
  }: ActionCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  for i in 0..layout.lpage_count {
    let lpage_dev_offset = state.data_allocator.allocate(state.pages.lpage_size());
    raw.write_u48_le_at(offsets.lpage(i), lpage_dev_offset);
  }
  for (i, tail_page_size_pow2) in layout.tail_page_sizes_pow2 {
    let page_dev_offset = state.data_allocator.allocate(1 << tail_page_size_pow2);
    raw.write_u48_le_at(offsets.tail_page(i), page_dev_offset);
  }

  let dev_offset = state.metadata_allocator.allocate(u64!(raw.len()));
  let obj = ObjectMetadata::new(
    dev_offset,
    raw.clone(),
    offsets,
    layout.tail_page_sizes_pow2,
  );
  let object_id = obj.id();
  let object_size = obj.size();

  let None = state.incomplete_objects.write().insert(object_id, AutoLifecycleObject::new(obj, ObjectState::Incomplete)) else {
    unreachable!();
  };

  state.metrics.incr_object_data_bytes(object_size);
  // NOTE: this is not the same as `req.raw.len()`, as that's padded to nearest page size.
  state
    .metrics
    .incr_object_metadata_bytes(offsets._total_size());

  txn.record(dev_offset, raw);

  Ok(OpCreateObjectOutput {
    token: IncompleteToken {
      partition_idx: state.partition_idx,
      object_id,
    },
  })
}
