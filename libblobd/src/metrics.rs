#[cfg(test)]
use crate::test_util::device::TestSeekableAsyncFile as SeekableAsyncFile;
#[cfg(test)]
use crate::test_util::journal::TestTransaction as Txn;
use off64::int::create_u64_be;
use off64::int::Off64AsyncReadInt;
use off64::usz;
#[cfg(not(test))]
use seekable_async_file::SeekableAsyncFile;
use serde::Serialize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
#[cfg(not(test))]
use write_journal::Transaction as Txn;

const OFFSETOF_ALLOCATED_BLOCK_COUNT: u64 = 0;
const OFFSETOF_ALLOCATED_PAGE_COUNT: u64 = OFFSETOF_ALLOCATED_BLOCK_COUNT + 8;
const OFFSETOF_DELETED_OBJECT_COUNT: u64 = OFFSETOF_ALLOCATED_PAGE_COUNT + 8;
const OFFSETOF_INCOMPLETE_OBJECT_COUNT: u64 = OFFSETOF_DELETED_OBJECT_COUNT + 8;
const OFFSETOF_OBJECT_COUNT: u64 = OFFSETOF_INCOMPLETE_OBJECT_COUNT + 8;
const OFFSETOF_OBJECT_DATA_BYTES: u64 = OFFSETOF_OBJECT_COUNT + 8;
const OFFSETOF_OBJECT_METADATA_BYTES: u64 = OFFSETOF_OBJECT_DATA_BYTES + 8;
const OFFSETOF_USED_BYTES: u64 = OFFSETOF_OBJECT_METADATA_BYTES + 8;

// Reserve more space for future proofing.
pub(crate) const METRICS_STATE_SIZE: u64 = 1024 * 1024 * 4;

#[derive(Serialize)]
pub struct BlobdMetrics {
  #[serde(skip_serializing)]
  dev_offset: u64,
  // This can only increase.
  allocated_block_count: AtomicU64,
  // This does not include metadata lpages.
  allocated_page_count: AtomicU64,
  deleted_object_count: AtomicU64,
  incomplete_object_count: AtomicU64,
  // This includes incomplete and deleted objects.
  object_count: AtomicU64,
  // To determine internal fragmentation, take `used_bytes` and subtract `object_data_bytes + object_metadata_bytes`. This is easier than attempting to track internal fragmentation directly.
  object_data_bytes: AtomicU64,
  object_metadata_bytes: AtomicU64,
  // Sum of non-free pages and block metadata lpages.
  used_bytes: AtomicU64,
}

#[rustfmt::skip]
impl BlobdMetrics {
  pub(crate) async fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64) -> Self {
    Self {
      dev_offset,
      allocated_block_count: dev.read_u64_be_at(dev_offset + OFFSETOF_ALLOCATED_BLOCK_COUNT).await.into(),
      allocated_page_count: dev.read_u64_be_at(dev_offset + OFFSETOF_ALLOCATED_PAGE_COUNT).await.into(),
      deleted_object_count: dev.read_u64_be_at(dev_offset + OFFSETOF_DELETED_OBJECT_COUNT).await.into(),
      incomplete_object_count: dev.read_u64_be_at(dev_offset + OFFSETOF_INCOMPLETE_OBJECT_COUNT).await.into(),
      object_count: dev.read_u64_be_at(dev_offset + OFFSETOF_OBJECT_COUNT).await.into(),
      object_data_bytes: dev.read_u64_be_at(dev_offset + OFFSETOF_OBJECT_DATA_BYTES).await.into(),
      object_metadata_bytes: dev.read_u64_be_at(dev_offset + OFFSETOF_OBJECT_METADATA_BYTES).await.into(),
      used_bytes: dev.read_u64_be_at(dev_offset + OFFSETOF_USED_BYTES).await.into(),
    }
  }

  pub(crate) async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at(dev_offset, vec![0u8; usz!(METRICS_STATE_SIZE)]).await;
  }

  pub fn allocated_block_count(&self) -> u64 { self.allocated_block_count.load(Relaxed) }
  pub fn allocated_page_count(&self) -> u64 { self.allocated_page_count.load(Relaxed) }
  pub fn deleted_object_count(&self) -> u64 { self.deleted_object_count.load(Relaxed) }
  pub fn incomplete_object_count(&self) -> u64 { self.incomplete_object_count.load(Relaxed) }
  pub fn object_count(&self) -> u64 { self.object_count.load(Relaxed) }
  pub fn object_data_bytes(&self) -> u64 { self.object_data_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.object_metadata_bytes.load(Relaxed) }
  pub fn used_bytes(&self) -> u64 { self.used_bytes.load(Relaxed) }

  pub(crate) fn incr_allocated_block_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_ALLOCATED_BLOCK_COUNT, create_u64_be(self.allocated_block_count.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_allocated_page_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_ALLOCATED_PAGE_COUNT, create_u64_be(self.allocated_page_count.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_deleted_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_DELETED_OBJECT_COUNT, create_u64_be(self.deleted_object_count.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_incomplete_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_INCOMPLETE_OBJECT_COUNT, create_u64_be(self.incomplete_object_count.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_COUNT, create_u64_be(self.object_count.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_object_data_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_DATA_BYTES, create_u64_be(self.object_data_bytes.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_object_metadata_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_METADATA_BYTES, create_u64_be(self.object_metadata_bytes.fetch_add(d, Relaxed) + d)); }
  pub(crate) fn incr_used_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_USED_BYTES, create_u64_be(self.used_bytes.fetch_add(d, Relaxed) + d)); }

  // We don't have a decrement method for `allocated_block_count` as it should never decrement.
  pub(crate) fn decr_allocated_page_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_ALLOCATED_PAGE_COUNT, create_u64_be(self.allocated_page_count.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_deleted_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_DELETED_OBJECT_COUNT, create_u64_be(self.deleted_object_count.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_incomplete_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_INCOMPLETE_OBJECT_COUNT, create_u64_be(self.incomplete_object_count.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_object_count(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_COUNT, create_u64_be(self.object_count.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_object_data_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_DATA_BYTES, create_u64_be(self.object_data_bytes.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_object_metadata_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_OBJECT_METADATA_BYTES, create_u64_be(self.object_metadata_bytes.fetch_sub(d, Relaxed) - d)); }
  pub(crate) fn decr_used_bytes(&self, txn: &mut Txn, d: u64) { txn.write(self.dev_offset + OFFSETOF_USED_BYTES, create_u64_be(self.used_bytes.fetch_sub(d, Relaxed) - d)); }
}
