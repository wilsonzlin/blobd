use super::serialisable::Serialisable;
use super::CollectionState;
use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::uring::Uring;
use dashmap::iter::Iter;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use futures::pin_mut;
use futures::StreamExt;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

// We need to split this off as otherwise it's impossible to use CollectionState mut methods while still sharing this DashMap with many threads (unless we use a lock, negating the purpose of DashMap).
pub(crate) struct PersistedDashMapReader<K: Copy + Eq + Hash + Serialisable, V: Serialisable> {
  map: DashMap<K, V, BuildHasherDefault<FxHasher>>,
}

impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> PersistedDashMapReader<K, V> {
  pub fn iter(&self) -> Iter<K, V, BuildHasherDefault<FxHasher>> {
    self.map.iter()
  }

  pub fn len(&self) -> usize {
    self.map.len()
  }

  pub fn get(&self, k: K) -> Option<Ref<K, V, BuildHasherDefault<FxHasher>>> {
    self.map.get(&k)
  }
}

pub(crate) struct PersistedDashMapWriter<K: Copy + Eq + Hash + Serialisable, V: Serialisable> {
  state: CollectionState<K, V>,
  reader: Arc<PersistedDashMapReader<K, V>>,
}

impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> Deref for PersistedDashMapWriter<K, V> {
  type Target = PersistedDashMapReader<K, V>;

  fn deref(&self) -> &Self::Target {
    &self.reader
  }
}

impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> PersistedDashMapWriter<K, V> {
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    key_deserialise_args: &K::DeserialiseArgs,
    value_deserialise_args: &V::DeserialiseArgs,
  ) -> (
    Arc<PersistedDashMapReader<K, V>>,
    PersistedDashMapWriter<K, V>,
  ) {
    let mut state = CollectionState::new(spage_size);
    let map = DashMap::default();
    {
      let stream = state.load_from_device(
        dev,
        first_page_dev_offset,
        key_deserialise_args,
        value_deserialise_args,
      );
      pin_mut!(stream);
      while let Some((k, v)) = stream.next().await {
        let None = map.insert(k, v) else {
          unreachable!();
        };
      }
    };
    let reader = Arc::new(PersistedDashMapReader { map });
    let writer = PersistedDashMapWriter {
      state,
      reader: reader.clone(),
    };
    (reader, writer)
  }

  // This function exists to provide the generic parameters to `CollectionState`.
  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    CollectionState::<K, V>::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    let state = &mut self.state;
    let map = &self.reader.map;
    state.commit(txn, alloc, |k| map.get(&k));
  }

  pub fn get_mut(&mut self, k: K) -> Option<RefMut<K, V, BuildHasherDefault<FxHasher>>> {
    let map = &self.reader.map;
    let state = &mut self.state;
    match map.get_mut(&k) {
      Some(v) => {
        state.entry_was_updated(k);
        Some(v)
      }
      None => None,
    }
  }

  pub fn insert(&mut self, k: K, v: V) -> Option<V> {
    let e = self.map.insert(k, v);
    match e {
      Some(_) => self.state.entry_was_updated(k),
      None => self.state.entry_was_inserted(k),
    };
    e
  }

  pub fn remove(&mut self, k: K) -> Option<V> {
    let e = self.map.remove(&k);
    if e.is_some() {
      self.state.entry_was_removed(k);
    };
    e.map(|e| e.1)
  }
}

impl<K: Copy + Eq + Hash + Serialisable, V: Default + Serialisable> PersistedDashMapWriter<K, V> {
  // We cannot provide `entry` directly because we don't know whether it'll be modified or not, even if we could guess it *probably* will.
  pub fn get_mut_or_insert_default(&mut self, k: K) -> RefMut<K, V, BuildHasherDefault<FxHasher>> {
    let map = &self.reader.map;
    let state = &mut self.state;
    let e = map.entry(k);
    if let Entry::Vacant(_) = e {
      state.entry_was_inserted(k);
    };
    e.or_default()
  }
}
