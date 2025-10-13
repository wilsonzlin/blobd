use crate::journal::IJournal;
use async_trait::async_trait;
use write_journal::Transaction;
use write_journal::WriteJournal;

#[async_trait]
impl IJournal for WriteJournal {
  async fn format_device(&self) {
    self.format_device().await;
  }

  async fn recover(&self) {
    self.recover().await;
  }

  async fn start_commit_background_loop(&self) {
    self.start_commit_background_loop().await;
  }

  fn begin_transaction(&self) -> Transaction {
    self.begin_transaction()
  }

  async fn commit_transaction(&self, txn: Transaction) {
    self.commit_transaction(txn).await;
  }
}
