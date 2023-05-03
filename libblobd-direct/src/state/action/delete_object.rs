use crate::journal::Transaction;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectState;
use crate::op::delete_object::OpDeleteObjectOutput;
use crate::op::OpResult;
use crate::state::State;
use crate::stream::StreamEventOwned;
use crate::stream::StreamEventType;
use crate::util::get_now_ms;
use tinybuf::TinyBuf;

pub(crate) struct ActionDeleteObjectInput {
  pub obj: AutoLifecycleObject,
}

pub(crate) fn action_delete_object(
  state: &mut State,
  txn: &mut Transaction,
  ActionDeleteObjectInput { obj }: ActionDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let now = get_now_ms();

  // We can reap the object now, as there should only be other readers (it's committed, so there shouldn't be any writers), and readers will double check the state after reading and discard the read if necessary (the object metadata still exists in memory due to Arc, so it's safe to check).

  let mut empty_page = state.pages.allocate_with_zeros(state.pages.spage_size());
  empty_page[0] = obj.metadata_page_size_pow2();
  empty_page[1] = ObjectState::_Empty as u8;
  txn.record(obj.dev_offset(), empty_page);

  for i in 0..obj.lpage_count() {
    let page_dev_offset = obj.lpage_dev_offset(i);
    state
      .data_allocator
      .release(page_dev_offset, state.pages.lpage_size_pow2);
  }
  for (i, tail_page_size_pow2) in obj.tail_page_sizes() {
    let page_dev_offset = obj.tail_page_dev_offset(i);
    state
      .data_allocator
      .release(page_dev_offset, tail_page_size_pow2);
  }
  // This is safe because we've recorded the empty page already.
  state
    .metadata_allocator
    .release(obj.dev_offset(), obj.metadata_page_size_pow2());

  state.metrics.decr_object_data_bytes(obj.size());
  state
    .metrics
    .decr_object_metadata_bytes(obj.metadata_size());

  state
    .stream
    .write()
    .add_event_pending_commit(StreamEventOwned {
      typ: StreamEventType::ObjectDelete,
      timestamp_ms: now,
      object_id: obj.id(),
      key: TinyBuf::from_slice(obj.key()),
    });

  Ok(OpDeleteObjectOutput {})
}
