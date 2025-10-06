use super::OpResult;
use crate::ctx::Ctx;
use crate::incomplete_token::IncompleteToken;
use crate::object::calc_object_layout;
use crate::object::Object;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::op::key_debug_str;
use crate::util::ceil_pow2;
use chrono::Utc;
use off64::u64;
use off64::u8;
use off64::Off64WriteMut;
use serde::Serialize;
use std::cmp::max;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tracing::trace;

pub struct OpCreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
}

pub struct OpCreateObjectOutput {
  pub token: IncompleteToken,
}

pub(crate) async fn op_create_object(
  ctx: Arc<Ctx>,
  req: OpCreateObjectInput,
) -> OpResult<OpCreateObjectOutput> {
  trace!(
    key = key_debug_str(&req.key),
    size = req.size,
    "creating object"
  );

  let layout = calc_object_layout(&ctx.pages, req.size);

  let mut lpage_dev_offsets = Vec::new();
  let mut tail_page_dev_offsets = Vec::new();
  {
    let mut allocator = ctx.heap_allocator.lock();
    for _ in 0..layout.lpage_count {
      let lpage_dev_offset = allocator.allocate(ctx.pages.lpage_size()).unwrap();
      lpage_dev_offsets.push(lpage_dev_offset);
    }
    for (_, tail_page_size_pow2) in layout.tail_page_sizes_pow2 {
      let page_dev_offset = allocator.allocate(1 << tail_page_size_pow2).unwrap();
      tail_page_dev_offsets.push(page_dev_offset);
    }
  };

  let metadata = ObjectMetadata {
    created: Utc::now(),
    key: req.key,
    size: req.size,
    lpage_dev_offsets,
    tail_page_dev_offsets,
  };

  let mut metadata_raw = Vec::new();
  metadata
    .serialize(&mut rmp_serde::Serializer::new(&mut metadata_raw))
    .unwrap();
  let metadata_size = u64!(metadata_raw.len());
  // TODO Check before allocating space and return `OpError::ObjectMetadataTooLarge`.
  assert!(metadata_size <= ctx.pages.lpage_size());
  let metadata_page_size = max(
    ctx.pages.spage_size(),
    ceil_pow2(metadata_size, ctx.pages.spage_size_pow2),
  );
  let metadata_dev_offset = ctx.heap_allocator.lock().allocate(metadata_size).unwrap();

  let object_id = ctx.next_object_id.fetch_add(1, Ordering::Relaxed);
  let tuple = ObjectTuple {
    id: object_id,
    state: ObjectState::Incomplete,
    metadata_dev_offset,
    metadata_page_size_pow2: u8!(metadata_page_size.ilog2()),
  };

  // NOTE: This is not the same as `allocate_from_data` as `metadata_page_size` may be much larger than the actual `metadata_size`.
  let mut write_page = ctx.pages.allocate_uninitialised(metadata_page_size);
  write_page.write_at(0, &metadata_raw);

  ctx.device.write_at(metadata_dev_offset, write_page).await;

  ctx.tuples.insert_object(tuple).await;

  // Out of abundance of caution, insert AFTER tuple is certain to have persisted.
  let None = ctx.incomplete_objects.write().insert(
    object_id,
    Object::new(object_id, ObjectState::Incomplete, metadata, metadata_size),
  ) else {
    unreachable!();
  };

  ctx
    .metrics
    .0
    .create_op_count
    .fetch_add(1, Ordering::Relaxed);
  ctx
    .metrics
    .0
    .incomplete_object_count
    .fetch_add(1, Ordering::Relaxed);
  ctx
    .metrics
    .0
    .object_metadata_bytes
    .fetch_add(metadata_size, Ordering::Relaxed);
  ctx
    .metrics
    .0
    .object_data_bytes
    .fetch_add(req.size, Ordering::Relaxed);

  trace!(
    id = object_id,
    size = req.size,
    metadata_dev_offset = metadata_dev_offset,
    metadata_page_size = metadata_page_size,
    "created object"
  );

  Ok(OpCreateObjectOutput {
    token: IncompleteToken {
      partition_idx: ctx.partition_idx,
      object_id,
    },
  })
}
