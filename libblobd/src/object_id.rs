use off64::create_u64_be;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tracing::debug;
use write_journal::Transaction;

pub(crate) struct ObjectIdSerial {
  dev_offset: u64,
  next: AtomicU64,
}

impl ObjectIdSerial {
  pub fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64) -> Self {
    let next = dev.read_at_sync(dev_offset, 8).read_u64_be_at(0);
    debug!(next_id = next, "object ID serial loaded");
    Self {
      dev_offset,
      next: AtomicU64::new(next),
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at(dev_offset, create_u64_be(0).to_vec()).await;
  }

  pub fn next(&self, txn: &mut Transaction) -> u64 {
    let id = self.next.fetch_add(1, Ordering::Relaxed);
    txn.write(self.dev_offset, create_u64_be(id + 1).to_vec());
    id
  }
}
