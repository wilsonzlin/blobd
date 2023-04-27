use super::serialisable::Serialisable;
use super::CollectionState;
use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::uring::Uring;
use futures::pin_mut;
use futures::StreamExt;
use rustc_hash::FxHashMap;
use std::hash::Hash;

pub(crate) struct PersistedHashMap<K: Copy + Eq + Hash + Serialisable, V: Serialisable> {
  state: CollectionState<K, V>,
  map: FxHashMap<K, V>,
}

impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> PersistedHashMap<K, V> {
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    key_deserialise_args: &K::DeserialiseArgs,
    value_deserialise_args: &V::DeserialiseArgs,
  ) -> Self {
    let mut state = CollectionState::new(spage_size);
    let mut map = FxHashMap::default();
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
    Self { state, map }
  }

  // This function exists to provide the generic parameters to `CollectionState`.
  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    CollectionState::<K, V>::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    let state = &mut self.state;
    let map = &self.map;
    state.commit(txn, alloc, |k| map.get(&k));
  }

  pub fn get(&self, k: K) -> Option<&V> {
    self.map.get(&k)
  }

  pub fn get_mut(&mut self, k: K) -> Option<&mut V> {
    match self.map.get_mut(&k) {
      Some(v) => {
        self.state.entry_was_updated(k);
        Some(v)
      }
      None => None,
    }
  }

  pub fn insert(&mut self, k: K, v: V) {
    match self.map.insert(k, v) {
      Some(_) => self.state.entry_was_updated(k),
      None => self.state.entry_was_inserted(k),
    }
  }

  pub fn remove(&mut self, k: K) -> Option<V> {
    let e = self.map.remove(&k);
    if e.is_some() {
      self.state.entry_was_removed(k);
    };
    e
  }
}
