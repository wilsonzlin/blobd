use super::device::TestSeekableAsyncFile;
use dashmap::DashMap;
use off64::usz;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use write_journal::tinybuf::TinyBuf;

pub struct TestOverlayEntry {
  pub data: TinyBuf,
  pub serial_no: u64,
}

pub struct TestTransactionWrite {
  pub offset: u64,
  pub data: TinyBuf,
  pub is_overlay: bool,
}

pub struct TestTransaction {
  pub serial_no: u64,
  pub writes: Vec<TestTransactionWrite>,
  pub overlay: Arc<DashMap<u64, TestOverlayEntry, BuildHasherDefault<FxHasher>>>,
}

impl TestTransaction {
  pub fn write<D: Into<TinyBuf>>(&mut self, offset: u64, data: D) -> &mut Self {
    let data = data.into();
    self.writes.push(TestTransactionWrite {
      offset,
      data,
      is_overlay: false,
    });
    self
  }

  /// WARNING: Use this function with caution, it's up to the caller to avoid the potential issues with misuse, including logic incorrectness, cache incoherency, and memory leaking. Carefully read notes/Overlay.md before using the overlay.
  pub fn write_with_overlay<D: Into<TinyBuf>>(&mut self, offset: u64, data: D) -> &mut Self {
    let data = data.into();
    self.overlay.insert(offset, TestOverlayEntry {
      data: data.clone(),
      serial_no: self.serial_no,
    });
    self.writes.push(TestTransactionWrite {
      offset,
      data,
      is_overlay: true,
    });
    self
  }
}

pub struct TestWriteJournal {
  pub device: TestSeekableAsyncFile,
  pub next_txn_serial_no: AtomicU64,
  pub committed: DashMap<u64, TestTransaction, BuildHasherDefault<FxHasher>>,
  pub overlay: Arc<DashMap<u64, TestOverlayEntry, BuildHasherDefault<FxHasher>>>,
}

impl TestWriteJournal {
  pub fn new(device: TestSeekableAsyncFile) -> Self {
    Self {
      device,
      next_txn_serial_no: AtomicU64::new(0),
      committed: Default::default(),
      overlay: Default::default(),
    }
  }

  pub async fn format_device(&self) {}

  pub async fn recover(&self) {}

  pub async fn start_commit_background_loop(&self) {
    loop {
      sleep(Duration::from_secs(1234567890)).await;
    }
  }

  pub fn begin_transaction(&self) -> TestTransaction {
    let serial_no = self.next_txn_serial_no.fetch_add(1, Ordering::Relaxed);
    TestTransaction {
      serial_no,
      writes: Vec::new(),
      overlay: self.overlay.clone(),
    }
  }

  pub async fn commit_transaction(&self, txn: TestTransaction) {
    let None = self.committed.insert(txn.serial_no, txn) else {
      unreachable!();
    };
  }

  pub async fn read_with_overlay(&self, offset: u64, len: u64) -> TinyBuf {
    if let Some(e) = self.overlay.get(&offset) {
      assert_eq!(e.value().data.len(), usz!(len));
      e.value().data.clone()
    } else {
      self.device.read_at(offset, len).await.into()
    }
  }
}
