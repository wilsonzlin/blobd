use crate::allocator::AllocDir;
use crate::allocator::Allocator;
use crate::backing_store::BoundedStore;
use crate::backing_store::PartitionStore;
use crate::object::calc_object_layout;
use crate::object::Object;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::object::OBJECT_TUPLE_SERIALISED_LEN;
use crate::pages::Pages;
use crate::tuples::Tuples;
use cadence::StatsdClient;
use dashmap::DashMap;
use num_traits::FromPrimitive;
use off64::u64;
use off64::usz;
use off64::Off64Read;
use std::cmp::max;
use std::cmp::min;
use std::collections::BTreeMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use tinybuf::TinyBuf;

// Map from object ID to bucket ID. It just happens so that object IDs are also chronological, so this map allows removing objects when they're committed and also popping chronologically.
pub(crate) type IncompleteObjects = Arc<parking_lot::RwLock<BTreeMap<u64, Object>>>;
// XXH3 should be a much higher quality hash than FxHasher.
pub(crate) type CommittedObjects =
  Arc<DashMap<TinyBuf, Object, BuildHasherDefault<twox_hash::xxh3::Hash64>>>;

pub(crate) struct LoadedObjects {
  pub committed_objects: CommittedObjects,
  pub heap_allocator: Allocator,
  pub incomplete_objects: IncompleteObjects,
  pub next_object_id: u64,
  pub tuples: Tuples,
}

pub(crate) async fn format_device_for_objects(tuples_area: BoundedStore, pages: &Pages) {
  // We need to erase the entire area so that even when new tuples and bundles are added the end is always ObjectState::_EndOfObjects.
  const BUFSIZE: u64 = 1024 * 1024 * 1024;
  for offset in (0..tuples_area.len()).step_by(usz!(BUFSIZE)) {
    let size = min(tuples_area.len() - offset, BUFSIZE);
    tuples_area
      .write_at(offset, pages.slow_allocate_with_zeros(size))
      .await;
  }
}

pub(crate) async fn load_objects_from_device(
  // This must not be bounded as we'll use raw partition absolute offsets.
  dev: PartitionStore,
  pages: Pages,
  statsd: Option<Arc<StatsdClient>>,
  heap_dev_offset: u64,
  heap_size: u64,
) -> LoadedObjects {
  let mut next_object_id = 0;
  let mut incomplete = BTreeMap::new();
  let committed: CommittedObjects = Default::default();
  let mut tuples = Vec::<Vec<ObjectTuple>>::new();
  let mut heap_allocator = Allocator::new(
    heap_dev_offset,
    heap_size,
    pages.clone(),
    AllocDir::Right,
    statsd.clone(),
  );

  let entire_tuples_area_raw = dev.read_at(0, heap_dev_offset).await;

  for bundle_id in 0..usz!(heap_dev_offset / pages.spage_size()) {
    let raw =
      entire_tuples_area_raw.read_at(u64!(bundle_id) * pages.spage_size(), pages.spage_size());
    let mut bundle_tuples = Vec::new();
    for tuple_raw in raw.chunks_exact(usz!(OBJECT_TUPLE_SERIALISED_LEN)) {
      let object_state = ObjectState::from_u8(tuple_raw[0]).unwrap();
      if object_state == ObjectState::_EndOfBundleTuples {
        break;
      };

      let tuple = ObjectTuple::deserialise(tuple_raw);
      let object_id = tuple.id;
      next_object_id = max(next_object_id, object_id + 1);

      heap_allocator.mark_as_allocated(tuple.metadata_dev_offset, tuple.metadata_page_size_pow2);
      let metadata_raw = dev
        .read_at(
          tuple.metadata_dev_offset,
          1 << tuple.metadata_page_size_pow2,
        )
        .await;
      // TODO Does rmp_serde know to stop at the end of the object, even if there's more bytes? Alternatively, we could use rmp_serde::from_read().
      let metadata: ObjectMetadata = rmp_serde::from_slice(metadata_raw.as_slice()).unwrap();

      let layout = calc_object_layout(&pages, metadata.size);

      let obj = Object::new(object_id, object_state, metadata);
      for &page_dev_offset in obj.lpage_dev_offsets.iter() {
        heap_allocator.mark_as_allocated(page_dev_offset, pages.lpage_size_pow2);
      }
      for (i, sz) in layout.tail_page_sizes_pow2 {
        let page_dev_offset = obj.tail_page_dev_offsets[usz!(i)];
        heap_allocator.mark_as_allocated(page_dev_offset, sz);
      }

      bundle_tuples.push(tuple);
      assert!(match object_state {
        ObjectState::Incomplete => incomplete.insert(obj.id(), obj),
        ObjectState::Committed => committed.insert(obj.key.clone(), obj),
        _ => unreachable!(),
      }
      .is_none());
    }
    tuples.push(bundle_tuples);
  }

  LoadedObjects {
    committed_objects: committed,
    heap_allocator,
    incomplete_objects: Arc::new(parking_lot::RwLock::new(incomplete)),
    next_object_id,
    tuples: Tuples::load_and_start(dev.bounded(0, heap_dev_offset), pages, tuples),
  }
}
