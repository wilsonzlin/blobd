use crate::device::mock::MockSeekableAsyncFile;
use crate::journal::IJournal;
use async_trait::async_trait;
use dashmap::DashMap;
use off64::usz;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;
use write_journal::OverlayEntry;
use write_journal::Transaction;

pub struct MockWriteJournal {
  pub device: MockSeekableAsyncFile,
  pub next_txn_serial_no: AtomicU64,
  pub committed: DashMap<u64, Transaction>,
  pub overlay: Arc<DashMap<u64, OverlayEntry>>,
}

impl MockWriteJournal {
  pub fn new(device: MockSeekableAsyncFile) -> Self {
    Self {
      device,
      next_txn_serial_no: AtomicU64::new(0),
      committed: Default::default(),
      overlay: Default::default(),
    }
  }
}

#[async_trait]
impl IJournal for MockWriteJournal {
  async fn format_device(&self) {}

  async fn recover(&self) {}

  async fn start_commit_background_loop(&self) {
    loop {
      sleep(Duration::from_secs(1234567890)).await;
    }
  }

  fn begin_transaction(&self) -> Transaction {
    let serial_no = self.next_txn_serial_no.fetch_add(1, Ordering::Relaxed);
    Transaction::new(serial_no, self.overlay.clone())
  }

  async fn commit_transaction(&self, txn: Transaction) {
    let None = self.committed.insert(txn.serial_no(), txn) else {
      unreachable!();
    };
  }

  async fn read_with_overlay(&self, offset: u64, len: u64) -> Vec<u8> {
    if let Some(e) = self.overlay.get(&offset) {
      assert_eq!(e.value().data.len(), usz!(len));
      e.value().data.clone()
    } else {
      self.device.read_at(offset, len).into()
    }
  }
}
