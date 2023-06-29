use crate::allocator::AllocDir;
use crate::allocator::Allocator;
use crate::backing_store::BoundedStore;
use crate::backing_store::PartitionStore;
use crate::metrics::BlobdMetrics;
use crate::object::calc_object_layout;
use crate::object::Object;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::pages::Pages;
use crate::tuples::load_raw_tuples_area_from_device;
use crate::tuples::load_tuples_from_raw_tuples_area;
use crate::tuples::tuple_bundles_count;
use crate::tuples::Tuples;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use off64::u64;
use off64::usz;
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::Deserialize;
use std::cmp::max;
use std::cmp::min;
use std::collections::BTreeMap;
use std::hash::BuildHasherDefault;
use std::io::Cursor;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tracing::warn;

// Map from object ID to bucket ID. It just happens so that object IDs are also chronological, so this map allows removing objects when they're committed and also popping chronologically.
pub(crate) type IncompleteObjects = Arc<RwLock<BTreeMap<u64, Object>>>;
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

/// Progress of initial loading of tuples and object metadata across all partitions.
#[derive(Default)]
pub(crate) struct ClusterLoadProgress {
  pub(crate) objects_loaded: AtomicU64,
  pub(crate) objects_total: AtomicU64,
  pub(crate) partitions_completed: AtomicUsize,
}

pub(crate) async fn load_objects_from_device(
  progress: Arc<ClusterLoadProgress>,
  // This must not be bounded as we'll use raw partition absolute offsets.
  dev: PartitionStore,
  pages: Pages,
  metrics: BlobdMetrics,
  heap_dev_offset: u64,
  heap_size: u64,
) -> LoadedObjects {
  let committed: Arc<CommittedObjects> = Default::default();
  let incomplete: Arc<RwLock<BTreeMap<u64, Object>>> = Default::default();
  let next_object_id: Arc<Mutex<u64>> = Default::default();
  let heap_allocator = Arc::new(Mutex::new(Allocator::new(
    heap_dev_offset,
    heap_size,
    pages.clone(),
    AllocDir::Right,
    metrics.clone(),
  )));

  let mut tuples: Vec<Vec<ObjectTuple>> =
    vec![Vec::new(); usz!(tuple_bundles_count(heap_dev_offset, &pages))];
  let raw_tuples_area = load_raw_tuples_area_from_device(&dev, heap_dev_offset).await;
  load_tuples_from_raw_tuples_area(&raw_tuples_area, &pages, |bundle_id, tuple| {
    tuples[usz!(bundle_id)].push(tuple)
  });

  // TODO Tune this concurrency value and make it configurable. Don't overwhelm the system memory or disk I/O queues, but go as fast as possible because this is slow.
  iter(tuples.iter()).for_each_concurrent(Some(1048576), |bundle_tuples| {
      let committed = committed.clone();
      let dev = dev.clone();
      let heap_allocator = heap_allocator.clone();
      let incomplete = incomplete.clone();
      let metrics = metrics.clone();
      let next_object_id = next_object_id.clone();
      let pages = pages.clone();
      let progress = progress.clone();

      async move {
        // We don't need to use for_each_concurrent, as we already have high parallelism in the outer bundles loop, and using it makes code a bit more complex due to needing locks and Arc clones.
        for tuple in bundle_tuples.iter() {
          progress.objects_total.fetch_add(1, Ordering::Relaxed);
          let object_state = tuple.state;
          let object_id = tuple.id;

          // Do not insert tuple yet, as we may drop it.

          let metadata_raw = dev
            .read_at(
              tuple.metadata_dev_offset,
              1 << tuple.metadata_page_size_pow2,
            )
            .await;
          progress.objects_loaded.fetch_add(1, Ordering::Relaxed);
          // Use a custom deserialiser so we can use a cursor, which will allow access to the rmp_serde::Deserializer::position method so we can determine the metadata byte size.
          let mut deserialiser = rmp_serde::Deserializer::new(Cursor::new(metadata_raw));
          // Yes, rmp_serde stops reading once fully deserialised, and doesn't error if there is extra junk afterwards.
          let metadata = ObjectMetadata::deserialize(&mut deserialiser).unwrap();
          let metadata_size = deserialiser.position();
          let object_size = metadata.size;

          let layout = calc_object_layout(&pages, object_size);

          let obj = Object::new(object_id, object_state, metadata, metadata_size);
          // Check if we should insert first before doing anything further e.g. updating metrics, updating allocator, pushing tuple. However, do update next_object_id; it's harmless to skip a few but dangerous to reuse: we still need to check assertions around IDs being unique and we may not actually delete duplicate objects before we accidentally reuse them.
          // We'll increment metrics for object counts at the end, in one addition.
          {
            let mut next_object_id = next_object_id.lock();
            *next_object_id = max(*next_object_id, object_id + 1);
          };
          match object_state {
            ObjectState::Incomplete => {
              assert!(incomplete.write().insert(object_id, obj.clone()).is_none());
            }
            ObjectState::Committed => {
              match committed.entry(obj.key.clone()) {
                Entry::Occupied(mut ent) => {
                  let existing_id = ent.get().id();
                  assert_ne!(existing_id, object_id);
                  let key_hex = hex::encode(&obj.key);
                  warn!(
                    key_hex,
                    older_object_id = min(existing_id, object_id),
                    newer_object_id = max(existing_id, object_id),
                    "multiple committed objects found with the same key, will only keep the latest committed one"
                  );
                  if existing_id > object_id {
                    continue;
                  };
                  ent.insert(obj.clone());
                }
                Entry::Vacant(vacant) => {
                  vacant.insert(obj.clone());
                }
              };
            }
            _ => unreachable!(),
          };

          {
            let mut heap_allocator = heap_allocator.lock();
            heap_allocator.mark_as_allocated(tuple.metadata_dev_offset, tuple.metadata_page_size_pow2);
            for &page_dev_offset in obj.lpage_dev_offsets.iter() {
              heap_allocator.mark_as_allocated(page_dev_offset, pages.lpage_size_pow2);
            }
            for (i, sz) in layout.tail_page_sizes_pow2 {
              let page_dev_offset = obj.tail_page_dev_offsets[usz!(i)];
              heap_allocator.mark_as_allocated(page_dev_offset, sz);
            }
          }

          metrics
            .0
            .object_metadata_bytes
            .fetch_add(metadata_size, Ordering::Relaxed);
          metrics
            .0
            .object_data_bytes
            .fetch_add(object_size, Ordering::Relaxed);
        }
      }
    })
    .await;
  progress
    .partitions_completed
    .fetch_add(1, Ordering::Relaxed);
  metrics
    .0
    .incomplete_object_count
    .fetch_add(u64!(incomplete.read().len()), Ordering::Relaxed);
  metrics
    .0
    .committed_object_count
    .fetch_add(u64!(committed.len()), Ordering::Relaxed);

  LoadedObjects {
    committed_objects: Arc::into_inner(committed).unwrap(),
    heap_allocator: Arc::into_inner(heap_allocator).unwrap().into_inner(),
    incomplete_objects: incomplete,
    next_object_id: Arc::into_inner(next_object_id).unwrap().into_inner(),
    tuples: Tuples::new(pages, tuples),
  }
}
