#![allow(unused)]

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;

pub(crate) struct KeyLock<K: Hash + Eq> {
  map: DashMap<K, Option<Arc<tokio::sync::RwLock<()>>>>,
}

impl<K: Hash + Eq> KeyLock<K> {
  pub fn new() -> Self {
    Self {
      map: Default::default(),
    }
  }

  pub fn get_lock(&self, k: K) -> Arc<tokio::sync::RwLock<()>> {
    self
      .map
      .entry(k)
      .or_insert_with(|| Some(Default::default()))
      .clone()
      .unwrap()
  }

  pub fn return_lock(&self, k: K) {
    let e = self.map.entry(k);
    let Entry::Occupied(mut e) = e else {
      // Already returned. This is possible because we call `return_lock` after dropping the Arc<RwLock<()>>, so another caller could've called this after we dropped the Arc but before we called `return_lock`.
      return;
    };
    let arc = e.get_mut().take().unwrap();
    match Arc::try_unwrap(arc) {
      Ok(_) => {
        e.remove();
      }
      Err(arc) => {
        *e.get_mut() = Some(arc);
      }
    };
  }
}
