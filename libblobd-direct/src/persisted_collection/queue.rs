use super::serialisable::Serialisable;
use super::CollectionState;
use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::uring::Uring;
use futures::pin_mut;
use futures::StreamExt;
use off64::usz;
use std::collections::BTreeMap;
use std::collections::VecDeque;

pub(crate) struct PersistedQueue<V: Serialisable> {
  state: CollectionState<u64, V>,
  deque: VecDeque<V>,
  head: u64,
  tail: u64,
}

impl<V: Serialisable> PersistedQueue<V> {
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    value_deserialise_args: &V::DeserialiseArgs,
  ) -> Self {
    let mut state = CollectionState::new(spage_size);
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
    let head = map.first_key_value().map(|e| *e.0).unwrap_or(0);
    let tail = map.last_key_value().map(|e| *e.0).unwrap_or(0);
    let mut last_key = None;
    let mut deque = VecDeque::new();
    for (k, v) in map {
      if last_key.is_some() && k != last_key.unwrap() + 1 {
        panic!("queue keys are not sequential");
      };
      last_key = Some(k);
      deque.push_back(v);
    }
    Self {
      state,
      deque,
      head,
      tail,
    }
  }

  // This function exists to provide the generic parameters to `CollectionState`.
  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    CollectionState::<u64, V>::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    self
      .state
      .commit(txn, alloc, |k| self.deque.get(usz!(k - self.head)));
  }

  pub fn pop(&mut self) -> Option<V> {
    let e = self.deque.pop_front();
    if e.is_some() {
      self.state.entry_was_removed(self.head);
      self.head += 1;
    };
    e
  }

  pub fn push(&mut self, v: V) {
    let id = self.tail;
    self.tail += 1;
    self.deque.push_back(v);
    self.state.entry_was_inserted(id);
  }
}
