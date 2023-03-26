use std::sync::atomic::{AtomicU64, Ordering};

use off64::{Off64Int, create_u64_be};
use seekable_async_file::SeekableAsyncFile;

pub struct ObjectIdSerial {
  dev_offset: u64,
  next: AtomicU64,
}

impl ObjectIdSerial {
  pub fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64) -> Self {
    let next = dev.read_at_sync(dev_offset, 8).read_u64_be_at(0);
    Self { dev_offset, next: AtomicU64::new(next) }
  }

  pub fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at_sync(dev_offset, create_u64_be(0).to_vec());
  }

  pub fn next(&self, mutation_writes: &mut Vec<(u64, Vec<u8>)>) -> u64 {
    let id = self.next.fetch_add(1, Ordering::Relaxed);
    mutation_writes.push((self.dev_offset, create_u64_be(id + 1).to_vec()));
    id
  }
}
