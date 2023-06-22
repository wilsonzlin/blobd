use super::OpError;
use super::OpResult;
use crate::ctx::Ctx;
use crate::object::calc_object_layout;
use crate::object::Object;
use off64::usz;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub(crate) async fn reap_object(ctx: &Ctx, obj: &Object) {
  let tuple = ctx.tuples.delete_object(obj.id()).await;

  let layout = calc_object_layout(&ctx.pages, obj.size);

  {
    let mut allocator = ctx.heap_allocator.lock();

    for &page_dev_offset in obj.lpage_dev_offsets.iter() {
      allocator.release(page_dev_offset, ctx.pages.lpage_size_pow2);
    }
    for (i, tail_page_size_pow2) in layout.tail_page_sizes_pow2 {
      let page_dev_offset = obj.tail_page_dev_offsets[usz!(i)];
      allocator.release(page_dev_offset, tail_page_size_pow2);
    }
    allocator.release(tuple.metadata_dev_offset, tuple.metadata_page_size_pow2);
  }
}

pub struct OpDeleteObjectInput {
  pub key: TinyBuf,
  // Only useful if versioning is enabled.
  pub id: Option<u64>,
}

pub struct OpDeleteObjectOutput {}

pub(crate) async fn op_delete_object(
  ctx: Arc<Ctx>,
  req: OpDeleteObjectInput,
) -> OpResult<OpDeleteObjectOutput> {
  let Some((_, obj)) = ctx.committed_objects.remove_if(&req.key, |_, o| req.id.is_none() || Some(o.id()) == req.id) else {
    return Err(OpError::ObjectNotFound);
  };

  // We can reap the object now, as there should only be other readers (it's committed, so there shouldn't be any writers), and readers will double check the state after reading and discard the read if necessary (the object metadata still exists in memory due to Arc, so it's safe to check).

  reap_object(&ctx, &obj).await;

  Ok(OpDeleteObjectOutput {})
}
