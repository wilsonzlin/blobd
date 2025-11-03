use crate::device::mock::MockDevice;
use crate::journal::IJournal;
use crate::journal::real::Transaction;
use async_trait::async_trait;
use dashmap::DashMap;
use signal_future::SignalFuture;

pub struct MockWriteJournal {
  pub device: MockDevice,
  pub committed: DashMap<u64, Vec<u8>>,
}

impl MockWriteJournal {
  pub fn new(device: MockDevice) -> Self {
    Self {
      device,
      committed: Default::default(),
    }
  }
}

#[async_trait]
impl IJournal for MockWriteJournal {
  async fn format_device(&self) {}

  async fn recover(&self) {}

  fn begin_transaction(&self) -> Transaction {
    Transaction::new()
  }

  fn commit_transaction(&self, txn: Transaction) -> Option<SignalFuture<()>> {
    for write in txn.into_writes() {
      self.committed.insert(write.offset, write.data);
    }
    None
  }
}
