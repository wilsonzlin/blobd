use crate::backing_store::BoundedStore;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::object::OBJECT_TUPLE_SERIALISED_LEN;
use crate::pages::Pages;
use off64::u16;
use off64::u64;
use off64::usz;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::time::sleep;

type ObjectId = u64;

#[derive(Debug)]
enum TupleChange {
  Insert(ObjectTuple),
  UpdateState(u64, ObjectState),
  Delete(u64),
}

#[derive(Default)]
struct TuplesState {
  object_id_to_bundle: FxHashMap<ObjectId, usize>,
  usage_to_bundles: BTreeMap<u16, FxHashSet<usize>>,
  bundle_usages: Vec<u16>,
}

type BundleUpdate = (TupleChange, SignalFutureController<Option<ObjectTuple>>);

pub(crate) struct Tuples {
  state: RwLock<TuplesState>,
  max_tuples_per_bundle: u16,
  // An ObjectTuple will only be provided via the signal for TupleChange::Delete.
  bundle_updates: Vec<mpsc::UnboundedSender<BundleUpdate>>,
}

impl Tuples {
  pub fn load_and_start(
    dev: BoundedStore,
    pages: Pages,
    bundles_with_initial_data: Vec<Vec<ObjectTuple>>,
  ) -> Self {
    let max_tuples_per_bundle = u16!(pages.spage_size() / OBJECT_TUPLE_SERIALISED_LEN);

    let mut state = TuplesState::default();
    let mut bundle_updates = Vec::new();
    for (bundle_id, tuples_init) in bundles_with_initial_data.into_iter().enumerate() {
      let mut tuples = FxHashMap::<u64, ObjectTuple>::default();
      for t in tuples_init {
        assert!(state.object_id_to_bundle.insert(t.id, bundle_id).is_none());
        assert!(tuples.insert(t.id, t).is_none());
      }
      state.bundle_usages.push(u16!(tuples.len()));
      assert!(state
        .usage_to_bundles
        .entry(u16!(tuples.len()))
        .or_default()
        .insert(bundle_id));

      let (sender, mut receiver) = mpsc::unbounded_channel::<BundleUpdate>();
      bundle_updates.push(sender);
      spawn({
        let dev = dev.clone();
        let pages = pages.clone();
        async move {
          // If this loop exits, it means we've dropped the `Tuple` and can safely stop.
          while let Some(msg) = receiver.recv().await {
            let mut changes = Vec::new();
            let mut signals = Vec::new();
            changes.push(msg.0);
            signals.push(msg.1);
            while let Some((change, signal)) = receiver.try_recv().ok() {
              changes.push(change);
              signals.push(signal);
            }
            let mut signal_values = Vec::new();
            for change in changes {
              signal_values.push(match change {
                TupleChange::Insert(t) => {
                  assert!(tuples.insert(t.id, t).is_none());
                  None
                }
                TupleChange::UpdateState(id, new_state) => {
                  tuples.get_mut(&id).unwrap().state = new_state;
                  None
                }
                TupleChange::Delete(id) => Some(tuples.remove(&id).unwrap()),
              });
            }
            let mut page = pages.allocate_uninitialised(pages.spage_size());
            let bundle_tuple_count = u16!(tuples.len());
            for (i, t) in tuples.values().enumerate() {
              let off = i * usz!(OBJECT_TUPLE_SERIALISED_LEN);
              t.serialise(&mut page[off..off + usz!(OBJECT_TUPLE_SERIALISED_LEN)]);
            }
            if bundle_tuple_count < max_tuples_per_bundle {
              page[usz!(bundle_tuple_count) * usz!(OBJECT_TUPLE_SERIALISED_LEN)] =
                ObjectState::_EndOfBundleTuples as u8;
            };
            dev
              .write_at(u64!(bundle_id) * pages.spage_size(), page)
              .await;
            for (i, signal_value) in signal_values.into_iter().enumerate() {
              signals[i].signal(signal_value);
            }
          }
        }
      });
    }
    Self {
      state: RwLock::new(state),
      max_tuples_per_bundle,
      bundle_updates,
    }
  }

  pub async fn insert_object(&self, o: ObjectTuple) {
    let sender = {
      let mut state = self.state.write();
      let (new_usage, bundle_id) = match state.usage_to_bundles.first_entry() {
        Some(mut e) if *e.key() < self.max_tuples_per_bundle => {
          let bundle_ids = e.get_mut();
          let bundle_id = *bundle_ids.iter().next().unwrap();
          assert!(bundle_ids.remove(&bundle_id));
          (*e.key() + 1, bundle_id)
        }
        _ => {
          panic!("run out of space for tuples")
        }
      };
      state.bundle_usages[bundle_id] = new_usage;
      assert!(state
        .usage_to_bundles
        .entry(new_usage)
        .or_default()
        .insert(bundle_id));
      assert!(state.object_id_to_bundle.insert(o.id, bundle_id).is_none());
      &self.bundle_updates[bundle_id]
    };
    let (fut, fut_ctl) = SignalFuture::new();
    assert!(sender.send((TupleChange::Insert(o), fut_ctl)).is_ok());
    fut.await;
  }

  pub async fn update_object_state(&self, object_id: u64, new_object_state: ObjectState) {
    let sender = {
      let state = self.state.read();
      let bundle_id = *state.object_id_to_bundle.get(&object_id).unwrap();
      &self.bundle_updates[bundle_id]
    };
    let (fut, fut_ctl) = SignalFuture::new();
    assert!(sender
      .send((
        TupleChange::UpdateState(object_id, new_object_state),
        fut_ctl,
      ))
      .is_ok());
    fut.await;
  }

  pub async fn delete_object(&self, object_id: u64) -> ObjectTuple {
    let sender = {
      let mut state = self.state.write();
      let bundle_id = *state.object_id_to_bundle.get(&object_id).unwrap();
      let old_usage = state.bundle_usages[bundle_id];
      state.bundle_usages[bundle_id] -= 1;
      assert!(state
        .usage_to_bundles
        .get_mut(&old_usage)
        .unwrap()
        .remove(&bundle_id));
      assert!(state
        .usage_to_bundles
        .entry(old_usage - 1)
        .or_default()
        .insert(bundle_id));
      &self.bundle_updates[bundle_id]
    };
    let (fut, fut_ctl) = SignalFuture::new();
    assert!(sender
      .send((TupleChange::Delete(object_id), fut_ctl))
      .is_ok());
    fut.await.unwrap()
  }
}
