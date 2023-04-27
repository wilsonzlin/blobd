use crate::journal::Transaction;
use crate::uring::Uring;
use bufpool::BUFPOOL;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use serde::Serialize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

const OFFSETOF_OBJECT_COUNT: u64 = 0;
const OFFSETOF_OBJECT_DATA_BYTES: u64 = OFFSETOF_OBJECT_COUNT + 8;
const OFFSETOF_OBJECT_METADATA_BYTES: u64 = OFFSETOF_OBJECT_DATA_BYTES + 8;
const OFFSETOF_USED_BYTES: u64 = OFFSETOF_OBJECT_METADATA_BYTES + 8;
const STATE_SIZE: u64 = OFFSETOF_USED_BYTES + 8;

#[derive(Serialize)]
struct Values {
  // This includes incomplete and deleted objects.
  object_count: AtomicU64,
  // To determine internal fragmentation, take `used_bytes` and subtract `object_data_bytes + object_metadata_bytes`. This is easier than attempting to track internal fragmentation directly.
  object_data_bytes: AtomicU64,
  object_metadata_bytes: AtomicU64,
  spage_size: u64,
  // Sum of non-free pages and block metadata lpages.
  used_bytes: AtomicU64,
}

pub struct BlobdMetrics {
  dev_offset: u64,
  dirty: AtomicBool,
  spage_size: u64,
  values: Values,
}

#[rustfmt::skip]
impl BlobdMetrics {
  pub(crate) async fn load_from_device(dev: Uring, dev_offset: u64, spage_size: u64) -> BlobdMetrics {
    assert!(spage_size >= STATE_SIZE);
    let raw = dev.read(dev_offset, spage_size).await;
    let values = Values {
      object_count: raw.read_u64_le_at(OFFSETOF_OBJECT_COUNT).into(),
      object_data_bytes: raw.read_u64_le_at(OFFSETOF_OBJECT_DATA_BYTES).into(),
      object_metadata_bytes: raw.read_u64_le_at(OFFSETOF_OBJECT_METADATA_BYTES).into(),
      spage_size,
      used_bytes: raw.read_u64_le_at(OFFSETOF_USED_BYTES).into(),
    };
    BlobdMetrics {
      dev_offset,
      dirty: AtomicBool::new(false),
      spage_size,
      values,
    }
  }

  pub(crate) async fn format_device(dev: Uring, dev_offset: u64, spage_size: u64) {
    dev.write(dev_offset, BUFPOOL.allocate_with_zeros(usz!(spage_size))).await;
  }

  pub(crate) fn commit(&self, txn: &mut Transaction) {
    // This isn't strictly thread-safe, as `self.dirty` could be changed after checking, but that's not a requirement, as we only call methods that change the state from within the serial state worker. We use atomics to avoid the need to split `BlobdMetrics` into a reader and writer type, and to avoid locks.
    if !self.dirty.load(Relaxed) {
      return;
    };
    self.dirty.store(false, Relaxed);

    let mut buf = BUFPOOL.allocate_with_zeros(usz!(self.spage_size));
    buf.write_u64_le_at(OFFSETOF_OBJECT_COUNT, self.object_count());
    buf.write_u64_le_at(OFFSETOF_OBJECT_DATA_BYTES, self.object_data_bytes());
    buf.write_u64_le_at(OFFSETOF_OBJECT_METADATA_BYTES, self.object_metadata_bytes());
    buf.write_u64_le_at(OFFSETOF_USED_BYTES, self.used_bytes());
    txn.record(self.dev_offset, buf);
  }

  pub fn object_count(&self) -> u64 { self.values.object_count.load(Relaxed) }
  pub fn object_data_bytes(&self) -> u64 { self.values.object_data_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.values.object_metadata_bytes.load(Relaxed) }
  pub fn used_bytes(&self) -> u64 { self.values.used_bytes.load(Relaxed) }

  pub(crate) fn incr_object_count(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_count.fetch_add(d, Relaxed); }
  pub(crate) fn incr_object_data_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_data_bytes.fetch_add(d, Relaxed); }
  pub(crate) fn incr_object_metadata_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_metadata_bytes.fetch_add(d, Relaxed); }
  pub(crate) fn incr_used_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.used_bytes.fetch_add(d, Relaxed); }

  pub(crate) fn decr_object_count(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_count.fetch_sub(d, Relaxed); }
  pub(crate) fn decr_object_data_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_data_bytes.fetch_sub(d, Relaxed); }
  pub(crate) fn decr_object_metadata_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.object_metadata_bytes.fetch_sub(d, Relaxed); }
  pub(crate) fn decr_used_bytes(&self, d: u64) { self.dirty.store(true, Relaxed); self.values.used_bytes.fetch_sub(d, Relaxed); }
}
