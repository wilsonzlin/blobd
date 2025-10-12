use crate::device::IDevice;
use off64::int::create_u64_be;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::debug;
use write_journal::Transaction;

// Why not just get max(object_id) + 1 on initial load? Because that may reuse IDs that were previously deleted.
pub(crate) struct ObjectIdSerial {
  dev_offset: u64,
  next: AtomicU64,
}

impl ObjectIdSerial {
  pub async fn load_from_device(dev: &Arc<dyn IDevice>, dev_offset: u64) -> Self {
    let next = dev.read_u64_be_at(dev_offset).await;
    debug!(next_id = next, "object ID serial loaded");
    Self {
      dev_offset,
      next: AtomicU64::new(next),
    }
  }

  pub async fn format_device(dev: &Arc<dyn IDevice>, dev_offset: u64) {
    dev.write_at(dev_offset, &create_u64_be(0)).await;
  }

  pub fn next(&self, txn: &mut Transaction) -> u64 {
    let id = self.next.fetch_add(1, Ordering::Relaxed);
    txn.write(self.dev_offset, create_u64_be(id + 1).to_vec());
    id
  }
}
