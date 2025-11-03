pub mod merge;
pub mod mock;
pub mod real;

use crate::journal::real::Transaction;
use async_trait::async_trait;
use signal_future::SignalFuture;

#[async_trait]
pub(crate) trait IJournal: Send + Sync {
  async fn format_device(&self);
  async fn recover(&self);
  fn begin_transaction(&self) -> Transaction;
  // This doesn't await the signal future itself because subtle ordering correctness semantics: the transaction must be applied to the journal while holding lock, but then lock can be released before actual async I/O completes.
  // This can return None if no write needs to be done.
  fn commit_transaction(&self, txn: Transaction) -> Option<SignalFuture<()>>;
}
