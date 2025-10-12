use crate::allocator::Allocator;
use crate::bucket::Buckets;
use crate::deleted_list::DeletedList;
use crate::device::IDevice;
use crate::incomplete_list::IncompleteList;
use crate::journal::IJournal;
use crate::metrics::BlobdMetrics;
use crate::object_header::Headers;
use crate::object_id::ObjectIdSerial;
use crate::page::Pages;
use parking_lot::Mutex as SyncMutex;
use std::sync::Arc;
use tokio::sync::Mutex;

// We must lock these together instead of individually. Inside a transaction, it will make mutation calls to these subsystems, and transactions get committed in the order they started. However, it's possible during the transaction that the earliest transaction does not reach all subsystems first, which would mean that the changes for some subsystems may get written out of order. For example, consider that request 1 may update incomplete list before request 0, even though request 0 came first, created an earlier transaction, and returned from its call to the free list before request 1, purely because of unfortunate luck with lock acquisition or the Tokio or Linux thread scheduler. (A simpler example would be if request 0 updates incomplete list first then allocator second, while request 1 updates allocator first then incomplete list second.) Request 0's transaction is always committed before request 1's (enforced by WriteJournal), but request 0 contains changes to incomplete list that depend on request 1's changes, so writing request 1 will clobber request 0's changes and corrupt the state.
pub(crate) struct State {
  pub deleted_list: DeletedList,
  pub incomplete_list: IncompleteList,
  pub object_id_serial: ObjectIdSerial,
}

pub(crate) struct Ctx {
  pub allocator: SyncMutex<Allocator>,
  pub buckets: Buckets,
  pub device: Arc<dyn IDevice>,
  /// WARNING: Do not call methods that mutate data on the device from outside a transaction and locked `State`. This isn't enforced via `&mut self` methods to save some hassle with the Rust borrow checker.
  pub headers: Headers,
  pub journal: Arc<dyn IJournal>,
  pub metrics: Arc<BlobdMetrics>,
  pub pages: Pages,
  // This value controls:
  // - How long tokens live for, which forces the object to live at least that long (even if marked as deleted).
  // - How long before incomplete objects are automatically sent for deletion.
  pub reap_objects_after_secs: u64,
  /// WARNING: Begin transaction AFTER acquiring lock, as otherwise state change data will be written out of order. The journal will always write transactions in order (even if committed out of order), which means transactions must be started in the order that state is changed, and that's not guaranteed if lock hasn't been acquired yet.
  pub state: Mutex<State>,
  pub versioning: bool,
}
