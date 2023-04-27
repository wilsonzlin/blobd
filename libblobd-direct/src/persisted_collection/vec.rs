use super::serialisable::Serialisable;
use super::CollectionState;
use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::uring::Uring;
use futures::pin_mut;
use futures::StreamExt;
use num::FromPrimitive;
use num::Integer;
use num::ToPrimitive;
use std::collections::BTreeMap;
use std::hash::Hash;

/// NOTE: This data structure only makes sense where keys are sequential and never removed.
/// Keys can only be integers, and there cannot be "holes" i.e. missing keys, so entries can never be removed.
pub(crate) struct PersistedVec<
  K: Copy + Hash + Integer + FromPrimitive + ToPrimitive + Serialisable,
  V: Serialisable,
> {
  state: CollectionState<K, V>,
  vec: Vec<V>,
}

impl<
    K: Copy + Hash + Integer + FromPrimitive + ToPrimitive + Serialisable<DeserialiseArgs = ()>,
    V: Serialisable,
  > PersistedVec<K, V>
{
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    value_deserialise_args: &V::DeserialiseArgs,
  ) -> Self {
    let mut state = CollectionState::<K, V>::new(spage_size);
    // CollectionState doesn't guarantee that entries are stored (and therefore retrieved) in order by key, as pages can be merged arbitrarily.
    let mut map = BTreeMap::new();
    {
      let stream = state.load_from_device(dev, first_page_dev_offset, &(), value_deserialise_args);
      pin_mut!(stream);
      while let Some((k, v)) = stream.next().await {
        let None = map.insert(k, v) else {
          unreachable!();
        };
      }
    };
    let mut last_key: Option<K> = None;
    let mut vec = Vec::new();
    for (k, v) in map {
      if last_key.is_some() && k != last_key.unwrap().add(K::one()) {
        panic!("Vec keys are not sequential");
      };
      last_key = Some(k);
      vec.push(v);
    }
    Self { state, vec }
  }

  // This function exists to provide the generic parameters to `CollectionState`.
  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    CollectionState::<K, V>::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    self
      .state
      .commit(txn, alloc, |k| self.vec.get(k.to_usize().unwrap()));
  }

  pub fn get(&self, k: K) -> Option<&V> {
    self.vec.get(k.to_usize().unwrap())
  }

  pub fn get_mut(&mut self, k: K) -> Option<&mut V> {
    let idx = k.to_usize().unwrap();
    match self.vec.get_mut(idx) {
      Some(v) => {
        self.state.entry_was_updated(k);
        Some(v)
      }
      None => None,
    }
  }

  pub fn set(&mut self, k: K, v: V) {
    let idx = k.to_usize().unwrap();
    self.vec[idx] = v;
    self.state.entry_was_updated(k);
  }

  pub fn push(&mut self, v: V) -> K {
    let idx = self.vec.len();
    self.vec.push(v);
    K::from_usize(idx).unwrap()
  }
}
