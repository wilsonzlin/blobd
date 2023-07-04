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
  pub(crate) object_count: AtomicU64, // Does not include written objects sitting in the log buffer.

  pub(crate) log_buffer_write_entry_count: AtomicU64,
  pub(crate) log_buffer_write_entry_data_bytes: AtomicU64,
  pub(crate) log_buffer_delete_entry_count: AtomicU64,
  pub(crate) log_buffer_virtual_head: AtomicU64,
  pub(crate) log_buffer_virtual_tail: AtomicU64,

  pub(crate) log_buffer_commit_count: AtomicU64,
  pub(crate) log_buffer_commit_entry_count: AtomicU64,
  pub(crate) log_buffer_commit_object_heap_move_count: AtomicU64, // How many objects were moved to the heap.
  pub(crate) log_buffer_commit_object_heap_move_write_us: AtomicU64, // How long it took to move objects to the heap.
  pub(crate) log_buffer_commit_bundle_committed_count: AtomicU64, // This only differs from `log_buffer_commit_bundle_count` while a commit is executing.
  pub(crate) log_buffer_commit_bundle_count: AtomicU64,
  pub(crate) log_buffer_commit_bundle_read_us: AtomicU64, // How long it took to read existing bundles from the device.
  pub(crate) log_buffer_commit_bundle_write_us: AtomicU64, // How long it took to write new bundles to the device.
  pub(crate) log_buffer_commit_total_us: AtomicU64,

  pub(crate) log_buffer_flush_entry_count: AtomicU64, // How many log buffer entries were flushed.
  pub(crate) log_buffer_flush_count: AtomicU64, // How many log buffer flushes occurred.
  pub(crate) log_buffer_flush_4k_count: AtomicU64, // How many flushes were [0, 4 KiB] in size excluding padding.
  pub(crate) log_buffer_flush_64k_count: AtomicU64, // How many flushes were (4 KiB, 64 KiB] in size excluding padding.
  pub(crate) log_buffer_flush_1m_count: AtomicU64, // How many flushes were (64 KiB, 1 MiB] in size excluding padding.
  pub(crate) log_buffer_flush_8m_count: AtomicU64, // How many flushes were (1 MiB, 8 MiB] in size excluding padding.
  pub(crate) log_buffer_flush_data_bytes: AtomicU64,
  pub(crate) log_buffer_flush_padding_bytes: AtomicU64,
  pub(crate) log_buffer_flush_write_us: AtomicU64, // How long it took to perform the log buffer flush's device write.
  pub(crate) log_buffer_flush_total_us: AtomicU64, // How long it took to perform the entire log buffer flush, including the device write and waiting for any antecedent flush to update the overlay and write the virtual tail.
  pub(crate) log_buffer_flush_state_count: AtomicU64, // How many times the log buffer virtual pointers were updated.

  pub(crate) uring_submission_count: AtomicU64,
  pub(crate) uring_read_request_count: AtomicU64,
  pub(crate) uring_read_request_bytes: AtomicU64,
  pub(crate) uring_read_request_us: AtomicU64,
  pub(crate) uring_write_request_count: AtomicU64,
  pub(crate) uring_write_request_bytes: AtomicU64,
  pub(crate) uring_write_request_us: AtomicU64,
  pub(crate) uring_sync_request_count: AtomicU64,
  pub(crate) uring_sync_request_us: AtomicU64,
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

  pub fn log_buffer_write_entry_count(&self) -> u64 { self.0.log_buffer_write_entry_count.load(Relaxed) }
  pub fn log_buffer_write_entry_data_bytes(&self) -> u64 { self.0.log_buffer_write_entry_data_bytes.load(Relaxed) }
  pub fn log_buffer_delete_entry_count(&self) -> u64 { self.0.log_buffer_delete_entry_count.load(Relaxed) }
  pub fn log_buffer_virtual_head(&self) -> u64 { self.0.log_buffer_virtual_head.load(Relaxed) }
  pub fn log_buffer_virtual_tail(&self) -> u64 { self.0.log_buffer_virtual_tail.load(Relaxed) }

  pub fn log_buffer_commit_count(&self) -> u64 { self.0.log_buffer_commit_count.load(Relaxed) }
  pub fn log_buffer_commit_entry_count(&self) -> u64 { self.0.log_buffer_commit_entry_count.load(Relaxed) }
  pub fn log_buffer_commit_object_heap_move_count(&self) -> u64 { self.0.log_buffer_commit_object_heap_move_count.load(Relaxed) }
  pub fn log_buffer_commit_object_heap_move_write_us(&self) -> u64 { self.0.log_buffer_commit_object_heap_move_write_us.load(Relaxed) }
  pub fn log_buffer_commit_bundle_committed_count(&self) -> u64 { self.0.log_buffer_commit_bundle_committed_count.load(Relaxed) }
  pub fn log_buffer_commit_bundle_count(&self) -> u64 { self.0.log_buffer_commit_bundle_count.load(Relaxed) }
  pub fn log_buffer_commit_bundle_read_us(&self) -> u64 { self.0.log_buffer_commit_bundle_read_us.load(Relaxed) }
  pub fn log_buffer_commit_bundle_write_us(&self) -> u64 { self.0.log_buffer_commit_bundle_write_us.load(Relaxed) }
  pub fn log_buffer_commit_total_us(&self) -> u64 { self.0.log_buffer_commit_total_us.load(Relaxed) }

  pub fn log_buffer_flush_entry_count(&self) -> u64 { self.0.log_buffer_flush_entry_count.load(Relaxed) }
  pub fn log_buffer_flush_count(&self) -> u64 { self.0.log_buffer_flush_count.load(Relaxed) }
  pub fn log_buffer_flush_4k_count(&self) -> u64 { self.0.log_buffer_flush_4k_count.load(Relaxed) }
  pub fn log_buffer_flush_64k_count(&self) -> u64 { self.0.log_buffer_flush_64k_count.load(Relaxed) }
  pub fn log_buffer_flush_1m_count(&self) -> u64 { self.0.log_buffer_flush_1m_count.load(Relaxed) }
  pub fn log_buffer_flush_8m_count(&self) -> u64 { self.0.log_buffer_flush_8m_count.load(Relaxed) }
  pub fn log_buffer_flush_data_bytes(&self) -> u64 { self.0.log_buffer_flush_data_bytes.load(Relaxed) }
  pub fn log_buffer_flush_padding_bytes(&self) -> u64 { self.0.log_buffer_flush_padding_bytes.load(Relaxed) }
  pub fn log_buffer_flush_write_us(&self) -> u64 { self.0.log_buffer_flush_write_us.load(Relaxed) }
  pub fn log_buffer_flush_total_us(&self) -> u64 { self.0.log_buffer_flush_total_us.load(Relaxed) }
  pub fn log_buffer_flush_state_count(&self) -> u64 { self.0.log_buffer_flush_state_count.load(Relaxed) }

  pub fn uring_submission_count(&self) -> u64 { self.0.uring_submission_count.load(Relaxed) }
  pub fn uring_read_request_count(&self) -> u64 { self.0.uring_read_request_count.load(Relaxed) }
  pub fn uring_read_request_bytes(&self) -> u64 { self.0.uring_read_request_bytes.load(Relaxed) }
  pub fn uring_read_request_us(&self) -> u64 { self.0.uring_read_request_us.load(Relaxed) }
  pub fn uring_write_request_count(&self) -> u64 { self.0.uring_write_request_count.load(Relaxed) }
  pub fn uring_write_request_bytes(&self) -> u64 { self.0.uring_write_request_bytes.load(Relaxed) }
  pub fn uring_write_request_us(&self) -> u64 { self.0.uring_write_request_us.load(Relaxed) }
  pub fn uring_sync_request_count(&self) -> u64 { self.0.uring_sync_request_count.load(Relaxed) }
  pub fn uring_sync_request_us(&self) -> u64 { self.0.uring_sync_request_us.load(Relaxed) }
}
