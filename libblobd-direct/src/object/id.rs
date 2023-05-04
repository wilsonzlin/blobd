use crate::journal::Transaction;
use crate::pages::Pages;
use crate::uring::UringBounded;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::debug;

struct Inner {
  dev: UringBounded,
  dirty: AtomicBool,
  next: AtomicU64,
  pages: Pages,
}

/// This can be cheaply cloned.
#[derive(Clone)]
pub(crate) struct ObjectIdSerial {
  inner: Arc<Inner>,
}

impl ObjectIdSerial {
  pub async fn load_from_device(dev: UringBounded, pages: Pages) -> Self {
    let raw = dev.read(0, pages.spage_size()).await;
    let next = raw.read_u64_le_at(0);
    debug!(next_id = next, "object ID serial loaded");
    Self {
      inner: Arc::new(Inner {
        dev,
        dirty: AtomicBool::new(false),
        next: AtomicU64::new(next),
        pages,
      }),
    }
  }

  pub async fn format_device(dev: UringBounded, pages: &Pages) {
    dev
      .write(0, pages.slow_allocate_with_zeros(pages.spage_size()))
      .await;
  }

  pub fn next(&self) -> u64 {
    let id = self.inner.next.fetch_add(1, Ordering::Relaxed);
    self.inner.dirty.store(true, Ordering::Relaxed);
    id
  }

  pub fn commit(&self, txn: &mut Transaction) {
    if !self.inner.dirty.swap(false, Ordering::Relaxed) {
      return;
    };
    // This is safe; we don't care if we skip a few IDs.
    let next = self.inner.next.load(Ordering::Relaxed);

    let mut raw = self
      .inner
      .pages
      .allocate_uninitialised(self.inner.pages.spage_size());
    raw.write_u64_le_at(0, next);
    self.inner.dev.record_in_transaction(txn, 0, raw);
  }
}
