use crate::bucket::Buckets;
use crate::incomplete_slots::IncompleteSlots;
use crate::object_id::ObjectIdSerial;
use crate::page::Pages;
use crate::stream::Stream;
use seekable_async_file::SeekableAsyncFile;
use tokio::sync::RwLock;
use write_journal::WriteJournal;

// We must lock these together instead of individually. Inside a transaction, it will make mutation calls to these subsystems, and transactions get committed in the order they started. However, it's possible during the transaction that the earliest transaction does not reach all subsystems first, which would mean that the changes for some subsystems may get written out of order. For example, consider that request 1 may update incomplete slots before request 0, even though request 0 came first, created an earlier transaction, and returned from its call to the free list before request 1, purely because of unfortunate luck with lock acquisition or the Tokio or Linux thread scheduler. Request 0's transaction is always committed before request 1's (enforced by WriteJournal), but request 0 contains changes to incomplete slots that depend on request 1's changes, so writing request 1 will clobber request 0's changes and corrupt the state.
pub(crate) struct State {
  pub incomplete_slots: IncompleteSlots,
  pub object_id_serial: ObjectIdSerial,
  pub pages: Pages,
  pub stream: Stream,
}

pub(crate) struct Ctx {
  pub buckets: Buckets,
  pub device: SeekableAsyncFile,
  pub journal: WriteJournal,
  pub state: RwLock<State>,
}
