use crate::allocator::AllocDir;
use crate::allocator::Allocator;
use crate::metrics::BlobdMetrics;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::pages::Pages;
use crate::uring::UringBounded;
use dashmap::DashMap;
use num_traits::FromPrimitive;
use off64::usz;
use std::cmp::min;
use std::collections::BTreeMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use tinybuf::TinyBuf;

// Map from object ID to bucket ID. It just happens so that object IDs are also chronological, so this map allows removing objects when they're committed and also popping chronologically.
pub(crate) type IncompleteObjects = Arc<parking_lot::RwLock<BTreeMap<u64, AutoLifecycleObject>>>;
// XXH3 should be a much higher quality hash than FxHasher.
pub(crate) type CommittedObjects =
  Arc<DashMap<TinyBuf, AutoLifecycleObject, BuildHasherDefault<twox_hash::xxh3::Hash64>>>;

pub(crate) struct LoadedObjects {
  pub incomplete_objects: IncompleteObjects,
  pub committed_objects: CommittedObjects,
  pub data_allocator: Allocator,
  pub metadata_allocator: Allocator,
}

pub(crate) async fn format_device_for_objects(metadata_dev: UringBounded, pages: &Pages) {
  // We need to erase the entire area so that even when new objects are added the end is always ObjectState::_EndOfObjects.
  const BUFSIZE: u64 = 1024 * 1024 * 1024;
  for offset in (0..metadata_dev.len()).step_by(usz!(BUFSIZE)) {
    let size = min(metadata_dev.len() - offset, 1024);
    metadata_dev
      .write(offset, pages.slow_allocate_with_zeros(size))
      .await;
  }
}

pub(crate) async fn load_objects_from_device(
  metadata_dev: UringBounded,
  pages: Pages,
  metrics: Arc<BlobdMetrics>,
  heap_dev_offset: u64,
  metadata_space: u64,
  data_space: u64,
) -> LoadedObjects {
  let meta_dev = metadata_dev;
  let mut incomplete = BTreeMap::new();
  let committed: CommittedObjects = Default::default();
  let mut meta_alloc = Allocator::new(
    heap_dev_offset,
    metadata_space,
    pages.clone(),
    AllocDir::Left,
    metrics.clone(),
  );
  let mut data_alloc = Allocator::new(
    heap_dev_offset + metadata_space,
    data_space,
    pages.clone(),
    AllocDir::Right,
    metrics.clone(),
  );

  let mut dev_offset = heap_dev_offset;
  // This condition handles the edge case where the entire metadata heap is used.
  'outer: while dev_offset < heap_dev_offset + metadata_space {
    let raw = meta_dev.read(dev_offset, pages.lpage_size()).await;
    let mut cur = &raw[..];
    while !cur.is_empty() {
      let page_size_pow2 = cur[0];
      let object_state = ObjectState::from_u8(cur[1]).unwrap();
      if object_state == ObjectState::_EndOfObjects {
        break 'outer;
      };
      meta_alloc.mark_as_allocated(dev_offset, page_size_pow2);

      let obj = AutoLifecycleObject::new(
        ObjectMetadata::load_from_raw(
          &pages,
          dev_offset,
          pages.allocate_from_data(&cur[..1 << page_size_pow2]),
        ),
        object_state,
      );
      for i in 0..obj.lpage_count() {
        let page_dev_offset = obj.lpage_dev_offset(i);
        data_alloc.mark_as_allocated(page_dev_offset, pages.lpage_size_pow2);
      }
      for (i, sz) in obj.tail_page_sizes() {
        let page_dev_offset = obj.tail_page_dev_offset(i);
        data_alloc.mark_as_allocated(page_dev_offset, sz);
      }

      assert!(match object_state {
        ObjectState::Incomplete => incomplete.insert(obj.id(), obj),
        ObjectState::Committed => committed.insert(TinyBuf::from_slice(obj.key()), obj),
        _ => unreachable!(),
      }
      .is_none());

      cur = &cur[1 << page_size_pow2..];
      dev_offset += 1 << page_size_pow2;
    }
  }

  LoadedObjects {
    committed_objects: committed,
    data_allocator: data_alloc,
    incomplete_objects: Arc::new(parking_lot::RwLock::new(incomplete)),
    metadata_allocator: meta_alloc,
  }
}
