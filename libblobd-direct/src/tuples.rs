use crate::backing_store::BoundedStore;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::object::OBJECT_TUPLE_SERIALISED_LEN;
use crate::pages::Pages;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use off64::u16;
use off64::u64;
use off64::usz;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::sleep;

type ObjectId = u64;

#[derive(Default)]
struct TuplesState {
  object_id_to_bundle: FxHashMap<ObjectId, usize>,
  bundle_usages: BTreeMap<u16, FxHashSet<usize>>,
  bundles: Vec<FxHashMap<ObjectId, ObjectTuple>>,
  dirty_bundles: FxHashMap<usize, Vec<SignalFutureController<()>>>,
}

#[derive(Clone)]
pub(crate) struct Tuples {
  state: Arc<Mutex<TuplesState>>,
  pages: Pages,
}

impl Tuples {
  pub fn new(pages: Pages, initial_data: Vec<(usize, ObjectTuple)>) -> Self {
    let mut state = TuplesState::default();
    for (bundle_id, tuple) in initial_data {
      assert!(state
        .object_id_to_bundle
        .insert(tuple.id, bundle_id)
        .is_none());
      while bundle_id >= state.bundles.len() {
        state.bundles.push(FxHashMap::default());
      }
      assert!(state.bundles[bundle_id].insert(tuple.id, tuple).is_none());
    }
    for (bundle_id, bundle) in state.bundles.iter().enumerate() {
      if !bundle.is_empty() {
        assert!(state
          .bundle_usages
          .entry(u16!(bundle.len()))
          .or_default()
          .insert(bundle_id));
      }
    }
    Self {
      state: Arc::new(Mutex::new(state)),
      pages,
    }
  }

  fn max_tuples_per_bundle(&self) -> u16 {
    u16!(self.pages.spage_size() / OBJECT_TUPLE_SERIALISED_LEN)
  }

  pub async fn insert_object(&self, o: ObjectTuple) {
    let fut = {
      let mut state = self.state.lock();
      let (new_usage, bundle_id) = match state.bundle_usages.pop_first() {
        Some((usage, mut bundle_ids)) if usage < self.max_tuples_per_bundle() => {
          let bundle_id = *bundle_ids.iter().next().unwrap();
          assert!(bundle_ids.remove(&bundle_id));
          if !bundle_ids.is_empty() {
            state.bundle_usages.insert(usage, bundle_ids);
          }
          (usage + 1, bundle_id)
        }
        e => {
          if let Some((k, v)) = e {
            state.bundle_usages.insert(k, v);
          };
          let id = state.bundles.len();
          state.bundles.push(FxHashMap::default());
          (1, id)
        }
      };
      assert!(state
        .bundle_usages
        .entry(new_usage)
        .or_default()
        .insert(bundle_id));
      assert!(state.object_id_to_bundle.insert(o.id, bundle_id).is_none());
      state.bundles[bundle_id].insert(o.id, o);
      let (fut, fut_ctl) = SignalFuture::<()>::new();
      state
        .dirty_bundles
        .entry(bundle_id)
        .or_default()
        .push(fut_ctl);
      fut
    };
    fut.await
  }

  pub async fn update_object_state(&self, object_id: u64, new_object_state: ObjectState) {
    let fut = {
      let mut state = self.state.lock();
      let bundle_id = *state.object_id_to_bundle.get(&object_id).unwrap();
      state.bundles[bundle_id].get_mut(&object_id).unwrap().state = new_object_state;
      let (fut, fut_ctl) = SignalFuture::<()>::new();
      state
        .dirty_bundles
        .entry(bundle_id)
        .or_default()
        .push(fut_ctl);
      fut
    };
    fut.await
  }

  pub async fn delete_object(&self, object_id: u64) -> ObjectTuple {
    let (tuple, fut) = {
      let mut state = self.state.lock();
      let bundle_id = *state.object_id_to_bundle.get(&object_id).unwrap();
      let old_usage = u16!(state.bundles[bundle_id].len());
      {
        let set = state.bundle_usages.get_mut(&old_usage).unwrap();
        assert!(set.remove(&bundle_id));
        if set.is_empty() {
          state.bundle_usages.remove(&old_usage).unwrap();
        };
      };
      let tuple = state.bundles[bundle_id].remove(&object_id).unwrap();
      assert!(state
        .bundle_usages
        .entry(old_usage - 1)
        .or_default()
        .insert(bundle_id));
      let (fut, fut_ctl) = SignalFuture::<()>::new();
      state
        .dirty_bundles
        .entry(bundle_id)
        .or_default()
        .push(fut_ctl);
      (tuple, fut)
    };
    fut.await;
    tuple
  }

  pub async fn start_background_commit_loop(
    &self,
    dev: BoundedStore,
  ) -> Vec<(usize, Vec<ObjectTuple>)> {
    loop {
      let mut changes = Vec::new();
      let mut signals = Vec::new();
      {
        let mut state = self.state.lock();
        // TODO Avoid collect_vec(), which is currently a workaround for Rust not letting us borrow `state` both mutably and immutably.
        for (bundle_id, mut bundle_signals) in state.dirty_bundles.drain().collect_vec() {
          let tuples = state.bundles[bundle_id].values().cloned().collect_vec();
          changes.push((bundle_id, tuples));
          signals.append(&mut bundle_signals);
        }
      };
      if changes.is_empty() {
        // TODO Tune.
        sleep(std::time::Duration::from_micros(100)).await;
        continue;
      };
      iter(changes)
        .for_each_concurrent(None, |(bundle_id, tuples)| {
          let dev = dev.clone();
          async move {
            let mut page = self.pages.allocate_uninitialised(self.pages.spage_size());
            let bundle_tuple_count = u16!(tuples.len());
            for (i, t) in tuples.into_iter().enumerate() {
              let off = i * usz!(OBJECT_TUPLE_SERIALISED_LEN);
              t.serialise(&mut page[off..off + usz!(OBJECT_TUPLE_SERIALISED_LEN)]);
            }
            if bundle_tuple_count < self.max_tuples_per_bundle() {
              page[usz!(bundle_tuple_count) * usz!(OBJECT_TUPLE_SERIALISED_LEN)] =
                ObjectState::_EndOfBundleTuples as u8;
            };
            dev
              .write_at(u64!(bundle_id) * self.pages.spage_size(), page)
              .await;
          }
        })
        .await;
      // WARNING: For any bundle, it's important to signal only after the bundle with the largest index has committed, as otherwise there could be gaps in the sequence of bundles if the process crashes (which split the tuples after the gap off) and cause acknowledged requests to get lost. Since this loop is the only code that writes to the tuples area, we can be safe that if we write the highest bundle, all lower bundles are contiguous.
      for signal in signals {
        signal.signal(());
      }
    }
  }
}
