use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

#[rustfmt::skip]
#[derive(Default)]
pub(crate) struct Inner {
  // For all `*_op_*` metrics, only successful calls are considered.
  pub(crate) commit_op_count: AtomicU64,
  pub(crate) create_op_count: AtomicU64,
  pub(crate) delete_op_count: AtomicU64,
  pub(crate) inspect_op_count: AtomicU64,

  pub(crate) read_op_count: AtomicU64,
  pub(crate) read_op_bytes_requested: AtomicU64,
  pub(crate) read_op_bytes_sent: AtomicU64,
  pub(crate) read_op_bytes_discarded: AtomicU64, // How many additional bytes were read from the device but not actually used due to alignment (i.e. read amplification).

  pub(crate) write_op_count: AtomicU64,
  pub(crate) write_op_bytes_requested: AtomicU64,
  pub(crate) write_op_bytes_written: AtomicU64,

  // To determine internal fragmentation, subtract `object_metadata_bytes + object_data_bytes` from this.
  pub(crate) allocated_bytes: AtomicU64,
  pub(crate) object_metadata_bytes: AtomicU64, // This includes incomplete objects.
  pub(crate) object_data_bytes: AtomicU64, // This includes incomplete objects.

  pub(crate) incomplete_object_count: AtomicU64,
  pub(crate) committed_object_count: AtomicU64,
}

#[derive(Clone, Default)]
pub struct BlobdMetrics(pub(crate) Arc<Inner>);

#[rustfmt::skip]
impl BlobdMetrics {
  pub fn commit_op_count(&self) -> u64 { self.0.commit_op_count.load(Relaxed) }
  pub fn create_op_count(&self) -> u64 { self.0.create_op_count.load(Relaxed) }
  pub fn delete_op_count(&self) -> u64 { self.0.delete_op_count.load(Relaxed) }
  pub fn inspect_op_count(&self) -> u64 { self.0.inspect_op_count.load(Relaxed) }

  pub fn read_op_count(&self) -> u64 { self.0.read_op_count.load(Relaxed) }
  pub fn read_op_bytes_requested(&self) -> u64 { self.0.read_op_bytes_requested.load(Relaxed) }
  pub fn read_op_bytes_sent(&self) -> u64 { self.0.read_op_bytes_sent.load(Relaxed) }
  pub fn read_op_bytes_discarded(&self) -> u64 { self.0.read_op_bytes_discarded.load(Relaxed) }

  pub fn write_op_count(&self) -> u64 { self.0.write_op_count.load(Relaxed) }
  pub fn write_op_bytes_requested(&self) -> u64 { self.0.write_op_bytes_requested.load(Relaxed) }
  pub fn write_op_bytes_written(&self) -> u64 { self.0.write_op_bytes_written.load(Relaxed) }

  pub fn allocated_bytes(&self) -> u64 { self.0.allocated_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.0.object_metadata_bytes.load(Relaxed) }
  pub fn object_data_bytes(&self) -> u64 { self.0.object_data_bytes.load(Relaxed) }

  pub fn incomplete_object_count(&self) -> u64 { self.0.incomplete_object_count.load(Relaxed) }
  pub fn committed_object_count(&self) -> u64 { self.0.committed_object_count.load(Relaxed) }
}
