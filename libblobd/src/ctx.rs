use crate::bucket::Buckets;
use crate::free_list::FreeList;
use crate::incomplete_slots::IncompleteSlots;
use crate::object_id::ObjectIdSerial;
use crate::stream::Stream;
use dashmap::DashMap;
use rustc_hash::FxHasher;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
use write_journal::AtomicWriteGroup;
use write_journal::WriteJournal;

// We must lock these together instead of individually. Inside a transaction, it will make mutation calls to these subsystems, and transactions get committed in the order they started. However, it's possible during the transaction that the earliest transaction does not reach all subsystems first, which would mean that the changes for some subsystems may get written out of order. For example, consider that request 1 may update incomplete slots before request 0, even though request 0 came first, created an earlier transaction, and returned from its call to the free list before request 1, purely because of unfortunate luck with lock acquisition or the Tokio or Linux thread scheduler. Request 0's transaction is always committed before request 1's (enforced by WriteJournal), but request 0 contains changes to incomplete slots that depend on request 1's changes, so writing request 1 will clobber request 0's changes and corrupt the state.
pub(crate) struct State {
  pub free_list: FreeList,
  pub incomplete_slots: IncompleteSlots,
  pub object_id_serial: ObjectIdSerial,
  pub stream: Stream,
}

pub(crate) struct Ctx {
  pub buckets: Buckets,
  pub device: SeekableAsyncFile,
  pub journal: WriteJournal,
  // We can safely use non-async lock as we shouldn't be holding this lock across await.
  pub state: parking_lot::RwLock<State>,
}
