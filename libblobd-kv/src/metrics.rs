use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

#[rustfmt::skip]
#[derive(Default)]
pub(crate) struct Inner {
  // For all `*_op_*` metrics, only successful calls are considered.
  pub(crate) delete_op_count: AtomicU64,

  pub(crate) read_op_count: AtomicU64,
  pub(crate) read_op_bytes_sent: AtomicU64,
  pub(crate) read_op_bytes_discarded: AtomicU64, // How many additional bytes were read from the device but not actually used due to alignment (i.e. read amplification).

  pub(crate) write_op_count: AtomicU64,
  pub(crate) write_op_bytes_persisted: AtomicU64,
  pub(crate) write_op_bytes_padding: AtomicU64, // How many additional unnecessary bytes were written to the device due to alignment (i.e. write amplification).

  // To determine internal fragmentation, subtract `heap_object_data_bytes` from this.
  pub(crate) allocated_bytes: AtomicU64,
  pub(crate) heap_object_data_bytes: AtomicU64, // i.e. does not include inline data.

  pub(crate) object_count: AtomicU64,
}

#[derive(Clone, Default)]
pub struct BlobdMetrics(pub(crate) Arc<Inner>);

#[rustfmt::skip]
impl BlobdMetrics {
  pub fn delete_op_count(&self) -> u64 { self.0.delete_op_count.load(Relaxed) }

  pub fn read_op_count(&self) -> u64 { self.0.read_op_count.load(Relaxed) }
  pub fn read_op_bytes_sent(&self) -> u64 { self.0.read_op_bytes_sent.load(Relaxed) }
  pub fn read_op_bytes_discarded(&self) -> u64 { self.0.read_op_bytes_discarded.load(Relaxed) }

  pub fn write_op_count(&self) -> u64 { self.0.write_op_count.load(Relaxed) }
  pub fn write_op_bytes_persisted(&self) -> u64 { self.0.write_op_bytes_persisted.load(Relaxed) }
  pub fn write_op_bytes_padding(&self) -> u64 { self.0.write_op_bytes_padding.load(Relaxed) }

  pub fn allocated_bytes(&self) -> u64 { self.0.allocated_bytes.load(Relaxed) }
  pub fn heap_object_data_bytes(&self) -> u64 { self.0.heap_object_data_bytes.load(Relaxed) }

  pub fn object_count(&self) -> u64 { self.0.object_count.load(Relaxed) }
}
