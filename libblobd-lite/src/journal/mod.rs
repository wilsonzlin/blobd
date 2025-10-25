pub mod mock;
pub mod real;

use async_trait::async_trait;
use write_journal::Transaction;

#[async_trait]
pub trait IJournal: Send + Sync {
  async fn format_device(&self);
  async fn recover(&self);
  fn begin_transaction(&self) -> Transaction;
  async fn commit_transaction(&self, txn: Transaction);
}
