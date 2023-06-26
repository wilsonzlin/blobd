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
use crate::object::OBJECT_TUPLE_SERIALISED_LEN;
use crate::pages::Pages;
use crate::tuples::Tuples;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use num_traits::FromPrimitive;
use off64::u64;
use off64::usz;
use off64::Off64Read;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
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
  let mut next_object_id = 0;
  let incomplete: Arc<RwLock<BTreeMap<u64, Object>>> = Default::default();
  let committed: Arc<Mutex<CommittedObjects>> = Default::default();
  let mut tuples: FxHashMap<u32, Vec<ObjectTuple>> = Default::default();
  let heap_allocator = Arc::new(Mutex::new(Allocator::new(
    heap_dev_offset,
    heap_size,
    pages.clone(),
    AllocDir::Right,
    metrics.clone(),
  )));

  let entire_tuples_area_raw = dev.read_at(0, heap_dev_offset).await;

  // TODO Tune this concurrency value and maybe make it configurable. Don't overwhelm the system memory or disk I/O queues, but go as fast as possible because this is slow.
  iter(0..usz!(heap_dev_offset / pages.spage_size()))
    .for_each_concurrent(Some(4096), |bundle_id| {
      let raw =
        entire_tuples_area_raw.read_at(u64!(bundle_id) * pages.spage_size(), pages.spage_size());
      let mut bundle_tuples = Vec::new();
      for tuple_raw in raw.chunks_exact(usz!(OBJECT_TUPLE_SERIALISED_LEN)) {
        let object_state = ObjectState::from_u8(tuple_raw[0]).unwrap();
        if object_state == ObjectState::_EndOfBundleTuples {
          break;
        };
        let tuple = ObjectTuple::deserialise(tuple_raw);
        heap_allocator
          .lock()
          .mark_as_allocated(tuple.metadata_dev_offset, tuple.metadata_page_size_pow2);
        next_object_id = max(next_object_id, tuple.id + 1);
        bundle_tuples.push(tuple);
      }
      progress
        .objects_total
        .fetch_add(bundle_tuples.len().try_into().unwrap(), Ordering::Relaxed);
      tuples.insert(bundle_id.try_into().unwrap(), bundle_tuples.clone());

      iter(bundle_tuples).for_each_concurrent(None, |tuple| {
        let committed = committed.clone();
        let dev = dev.clone();
        let heap_allocator = heap_allocator.clone();
        let incomplete = incomplete.clone();
        let pages = pages.clone();
        let metrics = metrics.clone();
        let progress = progress.clone();
        async move {
          let object_state = tuple.state;
          let object_id = tuple.id;

          let metadata_raw = dev
            .read_at(
              tuple.metadata_dev_offset,
              1 << tuple.metadata_page_size_pow2,
            )
            .await;
          progress.objects_loaded.fetch_add(1, Ordering::Relaxed);
          // Use a custom deserialiser so we can use a cursor, which will allow access to the rmp_serde::Deserializer::position method so we can determine the metadata byte size.
          let mut de = rmp_serde::Deserializer::new(Cursor::new(metadata_raw));
          // Yes, rmp_serde stops reading once fully deserialised, and doesn't error if there is extra junk afterwards.
          let metadata = ObjectMetadata::deserialize(&mut de).unwrap();
          let object_size = metadata.size;
          let metadata_size = de.position();

          let layout = calc_object_layout(&pages, object_size);

          let obj = Object::new(object_id, object_state, metadata, metadata_size);
          {
            let mut heap_allocator = heap_allocator.lock();
            for &page_dev_offset in obj.lpage_dev_offsets.iter() {
              heap_allocator.mark_as_allocated(page_dev_offset, pages.lpage_size_pow2);
            }
            for (i, sz) in layout.tail_page_sizes_pow2 {
              let page_dev_offset = obj.tail_page_dev_offsets[usz!(i)];
              heap_allocator.mark_as_allocated(page_dev_offset, sz);
            }
          }

          // We'll increment metrics for object counts at the end, in one addition.
          match object_state {
            ObjectState::Incomplete => {
              assert!(incomplete.write().insert(object_id, obj).is_none());
            }
            ObjectState::Committed => {
              match committed.lock().entry(obj.key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(mut ent) => {
                  let existing_id = ent.get().id();
                  assert_ne!(existing_id, object_id);
                  let key_hex = hex::encode(&obj.key);
                  warn!(
                    key_hex,
                    older_object_id = min(existing_id, object_id),
                    newer_object_id = max(existing_id, object_id),
                    "multiple committed objects found with the same key, will only keep the latest committed one"
                  );
                  if existing_id < object_id {
                    ent.insert(obj);
                  };
                }
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                  vacant.insert(obj);
                }
              };
            }
            _ => unreachable!(),
          };

          metrics
            .0
            .object_metadata_bytes
            .fetch_add(metadata_size, Ordering::Relaxed);
          metrics
            .0
            .object_data_bytes
            .fetch_add(object_size, Ordering::Relaxed);
        }
      })
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
    .fetch_add(u64!(committed.lock().len()), Ordering::Relaxed);

  fn unwrap_arc_mutex<T>(v: Arc<Mutex<T>>) -> T {
    let Ok(v) = Arc::try_unwrap(v) else {
      unreachable!();
    };
    v.into_inner()
  }

  LoadedObjects {
    committed_objects: unwrap_arc_mutex(committed),
    heap_allocator: unwrap_arc_mutex(heap_allocator),
    incomplete_objects: incomplete,
    next_object_id,
    tuples: Tuples::new(pages, tuples),
  }
}
