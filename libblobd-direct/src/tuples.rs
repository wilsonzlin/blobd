use crate::backing_store::BoundedStore;
use crate::backing_store::PartitionStore;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::object::OBJECT_TUPLE_SERIALISED_LEN;
use crate::pages::Pages;
use bufpool::buf::Buf;
use croaring::Bitmap;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use num_traits::FromPrimitive;
use off64::u16;
use off64::u32;
use off64::u64;
use off64::usz;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::sync::Arc;
use tokio::time::sleep;

type ObjectId = u64;
pub(crate) type BundleId = u32;

#[derive(Default)]
struct TuplesState {
  object_id_to_bundle: FxHashMap<ObjectId, BundleId>,
  bundle_tuples: Vec<FxHashMap<ObjectId, ObjectTuple>>,
  // These bitmaps provide some features:
  // - Quickly find a dirty bundle to add a new tuple to, to try and coalesce writes.
  // - Try to keep picking the same dirty bundle (`.minimum()`) while it's still free to add new tuples to, to further try and coalesce writes. Deletes may interfere with this.
  // - Provide quick set-like insertion and deletion.
  // Any dirty tuple is as good as any other, so there's no need for an ordered (by usage) data structure.
  free_bundles: Bitmap,
  dirty_bundles: Bitmap,
  free_and_dirty_bundles: Bitmap,
  dirty_signals: Vec<SignalFutureController<()>>,
}

#[derive(Clone)]
pub(crate) struct Tuples {
  state: Arc<Mutex<TuplesState>>,
  max_tuples_per_bundle: u16,
}

impl Tuples {
  pub fn new(pages: Pages, bundles_with_initial_data: Vec<Vec<ObjectTuple>>) -> Self {
    let max_tuples_per_bundle = u16!(pages.spage_size() / OBJECT_TUPLE_SERIALISED_LEN);

    let mut state = TuplesState::default();
    for (bundle_id, tuples_init) in bundles_with_initial_data.into_iter().enumerate() {
      let bundle_id = u32!(bundle_id);
      let tuple_count = u16!(tuples_init.len());
      assert!(tuple_count <= max_tuples_per_bundle);
      let mut tuples = FxHashMap::<u64, ObjectTuple>::default();
      for t in tuples_init {
        assert!(state.object_id_to_bundle.insert(t.id, bundle_id).is_none());
        assert!(tuples.insert(t.id, t).is_none());
      }
      state.bundle_tuples.push(tuples);
      if tuple_count < max_tuples_per_bundle {
        state.free_bundles.add(bundle_id);
      };
    }
    Self {
      state: Arc::new(Mutex::new(state)),
      max_tuples_per_bundle,
    }
  }

  pub async fn insert_object(&self, o: ObjectTuple) {
    let fut = {
      let mut state = self.state.lock();
      let bundle_id = state
        .free_and_dirty_bundles
        .minimum()
        .or_else(|| state.free_bundles.minimum())
        .expect("run out of space for tuples");
      assert!(state.object_id_to_bundle.insert(o.id, bundle_id).is_none());
      let bundle = &mut state.bundle_tuples[usz!(bundle_id)];
      assert!(bundle.insert(o.id, o).is_none());
      assert!(u16!(bundle.len()) <= self.max_tuples_per_bundle);
      if u16!(bundle.len()) == self.max_tuples_per_bundle {
        state.free_and_dirty_bundles.remove(bundle_id);
        state.free_bundles.remove(bundle_id);
      } else {
        state.free_and_dirty_bundles.add(bundle_id);
      }
      state.dirty_bundles.add(bundle_id);
      let (fut, fut_ctl) = SignalFuture::new();
      state.dirty_signals.push(fut_ctl);
      fut
    };
    fut.await;
  }

  pub async fn update_object_id_and_state(
    &self,
    cur_object_id: u64,
    new_object_id: u64,
    new_object_state: ObjectState,
  ) {
    let fut = {
      let mut state = self.state.lock();
      let bundle_id = state.object_id_to_bundle.remove(&cur_object_id).unwrap();
      let bundle = &mut state.bundle_tuples[usz!(bundle_id)];
      let bundle_len = u16!(bundle.len());
      let mut tuple = bundle.remove(&cur_object_id).unwrap();
      tuple.id = new_object_id;
      tuple.state = new_object_state;
      assert!(bundle.insert(new_object_id, tuple).is_none());
      assert!(state
        .object_id_to_bundle
        .insert(new_object_id, bundle_id)
        .is_none());
      // We didn't add any tuples, but we did update one, making this bundle dirty and eligible for `free_and_dirty_bundles`.
      if bundle_len < self.max_tuples_per_bundle {
        state.free_and_dirty_bundles.add(bundle_id);
      }
      state.dirty_bundles.add(bundle_id);
      let (fut, fut_ctl) = SignalFuture::new();
      state.dirty_signals.push(fut_ctl);
      fut
    };
    fut.await;
  }

  pub async fn delete_object(&self, object_id: u64) -> ObjectTuple {
    let (tuple, fut) = {
      let mut state = self.state.lock();
      let bundle_id = state.object_id_to_bundle.remove(&object_id).unwrap();
      let tuple = state.bundle_tuples[usz!(bundle_id)]
        .remove(&object_id)
        .unwrap();
      state.free_and_dirty_bundles.add(bundle_id);
      state.dirty_bundles.add(bundle_id);
      let (fut, fut_ctl) = SignalFuture::new();
      state.dirty_signals.push(fut_ctl);
      (tuple, fut)
    };
    fut.await;
    tuple
  }

  // One loop with some delay works better than one tight loop per bundle (i.e. one async loop and queue per sector size) as this allows for batching, increasing opportunities for sequential and coalesced writes further down the stack (kernel, iSCSI/NVMe, disk controller, etc.). Many random writes of the smallest (i.e. sector) size is NOT peak performance.
  pub async fn start_background_commit_loop(&self, dev: BoundedStore, pages: Pages) {
    loop {
      let mut changes = Vec::new();
      // TODO Coalesce writes if nearest aligned power of two has high ratio of changed tuples. For example, if bundles 9, 10, 11, 12, 13, and 15 have changed, write bundles 8 to 15 (inclusive) as one page to offset of bundle 8, including bundles 8 and 14 even though they haven't changed.
      let signals = {
        let mut state = self.state.lock();
        for bundle_id in state.dirty_bundles.iter() {
          let tuples = state.bundle_tuples[usz!(bundle_id)]
            .values()
            .cloned()
            .collect_vec();
          changes.push((bundle_id, tuples));
        }
        state.dirty_bundles.clear();
        state.free_and_dirty_bundles.clear();
        state.dirty_signals.drain(..).collect_vec()
      };
      if changes.is_empty() {
        // TODO Tune, allow configuration.
        sleep(std::time::Duration::from_micros(10)).await;
        continue;
      };
      iter(changes)
        .for_each_concurrent(None, |(bundle_id, tuples)| {
          let dev = dev.clone();
          let pages = pages.clone();
          async move {
            let mut page = pages.allocate_uninitialised(pages.spage_size());
            let bundle_tuple_count = u16!(tuples.len());
            for (i, t) in tuples.into_iter().enumerate() {
              let off = i * usz!(OBJECT_TUPLE_SERIALISED_LEN);
              t.serialise(&mut page[off..off + usz!(OBJECT_TUPLE_SERIALISED_LEN)]);
            }
            if bundle_tuple_count < self.max_tuples_per_bundle {
              page[usz!(bundle_tuple_count) * usz!(OBJECT_TUPLE_SERIALISED_LEN)] =
                ObjectState::_EndOfBundleTuples as u8;
            };
            dev
              .write_at(u64!(bundle_id) * pages.spage_size(), page)
              .await;
          }
        })
        .await;
      for signal in signals {
        signal.signal(());
      }
    }
  }
}

pub(crate) async fn load_raw_tuples_area_from_device(
  dev: &PartitionStore,
  heap_dev_offset: u64,
) -> Buf {
  dev.read_at(0, heap_dev_offset).await
}

pub(crate) fn tuple_bundles_count(heap_dev_offset: u64, pages: &Pages) -> u64 {
  assert_eq!(heap_dev_offset % pages.spage_size(), 0);
  heap_dev_offset / pages.spage_size()
}

pub(crate) fn load_tuples_from_raw_tuples_area(
  entire_tuples_area_raw: &[u8],
  pages: &Pages,
  mut on_tuple: impl FnMut(BundleId, ObjectTuple),
) {
  assert_eq!(entire_tuples_area_raw.len() % usz!(pages.spage_size()), 0);
  for (bundle_id, raw) in entire_tuples_area_raw
    .chunks_exact(usz!(pages.spage_size()))
    .enumerate()
  {
    for tuple_raw in raw.chunks_exact(usz!(OBJECT_TUPLE_SERIALISED_LEN)) {
      let object_state = ObjectState::from_u8(tuple_raw[0]).unwrap();
      if object_state == ObjectState::_EndOfBundleTuples {
        break;
      };
      let tuple = ObjectTuple::deserialise(tuple_raw);
      on_tuple(u32!(bundle_id), tuple);
    }
  }
}
