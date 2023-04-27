pub mod action;

use self::action::commit_object::action_commit_object;
use self::action::commit_object::ActionCommitObjectInput;
use self::action::create_object::action_create_object;
use self::action::create_object::ActionCreateObjectInput;
use self::action::delete_object::action_delete_object;
use self::action::delete_object::ActionDeleteObjectInput;
use crate::allocator::Allocator;
use crate::bucket::BucketsWriter;
use crate::incomplete_list::IncompleteListWriter;
use crate::journal::Journal;
use crate::journal::Transaction;
use crate::metrics::BlobdMetrics;
use crate::object::id::ObjectIdSerial;
use crate::object::ObjectMetadata;
use crate::op::commit_object::OpCommitObjectOutput;
use crate::op::create_object::OpCreateObjectOutput;
use crate::op::delete_object::OpDeleteObjectOutput;
use crate::op::OpResult;
use crate::stream::CommittedEvents;
use crate::stream::Stream;
use crate::BlobdCfg;
use bufpool::buf::Buf;
use dashmap::DashMap;
use rustc_hash::FxHasher;
use signal_future::SignalFutureController;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::{self};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Duration;

// We have actions that change state relating to deleting objects (e.g. incomplete reaper, delete object op).
// However, we can't commit the state changes until the object actually drops and is reaped.
// Usually, we're waiting on soem read/write stream device I/O request. It's really really short, so the actual chance of a race condition is almost zero, but still requires consideration for correctness.
// We want to avoid locks, and also waiting on them in our tight single-threaded synchronous state worker.
// Therefore, we insert an object ID here when we perform some state change that depends on an object being reaped, and remove an ID here from the Drop::drop method. (Both must assert that the entry does/doesn't exist for correctness.) On every loop, we check that this is empty before proceeding, as otherwise we must wait and we don't want to wait.
pub(crate) type ObjectsPendingDrop = DashMap<u64, ObjectMetadata, BuildHasherDefault<FxHasher>>;

// We must lock these together instead of individually. Inside a transaction, it will make mutation calls to these subsystems, and transactions get committed in the order they started. However, it's possible during the transaction that the earliest transaction does not reach all subsystems first, which would mean that the changes for some subsystems may get written out of order. For example, consider that request 1 may update incomplete list before request 0, even though request 0 came first, created an earlier transaction, and returned from its call to the free list before request 1, purely because of unfortunate luck with lock acquisition or the Tokio or Linux thread scheduler. (A simpler example would be if request 0 updates incomplete list first then allocator second, while request 1 updates allocator first then incomplete list second.) Request 0's transaction is always committed before request 1's (enforced by WriteJournal), but request 0 contains changes to incomplete list that depend on request 1's changes, so writing request 1 will clobber request 0's changes and corrupt the state.
pub(crate) struct State {
  pub allocator: Allocator,
  pub buckets: BucketsWriter,
  pub cfg: BlobdCfg,
  pub incomplete_list: IncompleteListWriter,
  pub metrics: Arc<BlobdMetrics>,
  pub object_id_serial: ObjectIdSerial,
  pub stream: Arc<parking_lot::RwLock<Stream>>,
}

pub(crate) struct StateWorker {
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  action_sender: crossbeam_channel::Sender<StateAction>,
}

pub(crate) enum StateAction {
  Commit(
    ActionCommitObjectInput,
    SignalFutureController<OpResult<OpCommitObjectOutput>>,
  ),
  Create(
    ActionCreateObjectInput,
    SignalFutureController<OpResult<OpCreateObjectOutput>>,
  ),
  Delete(
    ActionDeleteObjectInput,
    SignalFutureController<OpResult<OpDeleteObjectOutput>>,
  ),
}

enum SignalReadyToFire {
  Commit(
    SignalFutureController<OpResult<OpCommitObjectOutput>>,
    OpResult<OpCommitObjectOutput>,
  ),
  Create(
    SignalFutureController<OpResult<OpCreateObjectOutput>>,
    OpResult<OpCreateObjectOutput>,
  ),
  Delete(
    SignalFutureController<OpResult<OpDeleteObjectOutput>>,
    OpResult<OpDeleteObjectOutput>,
  ),
}

impl Debug for SignalReadyToFire {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Commit(_, _) => f.debug_tuple("Commit").finish(),
      Self::Create(_, _) => f.debug_tuple("Create").finish(),
      Self::Delete(_, _) => f.debug_tuple("Delete").finish(),
    }
  }
}

impl StateAction {
  fn execute(
    self,
    state: &mut State,
    txn: &mut Transaction,
    signals_ready_to_fire: &mut Vec<SignalReadyToFire>,
  ) {
    signals_ready_to_fire.push(match self {
      StateAction::Commit(i, s) => SignalReadyToFire::Commit(s, action_commit_object(state, i)),
      StateAction::Create(i, s) => {
        SignalReadyToFire::Create(s, action_create_object(state, txn, i))
      }
      StateAction::Delete(i, s) => SignalReadyToFire::Delete(s, action_delete_object(state, i)),
    });
  }
}

impl StateWorker {
  pub fn start(
    mut state: State,
    journal: Journal,
    objects_pending_drop: Arc<ObjectsPendingDrop>,
  ) -> Self {
    let (txn_sender, mut txn_receiver) = tokio::sync::mpsc::unbounded_channel::<(
      VecDeque<(u64, Buf)>,
      CommittedEvents,
      Vec<SignalReadyToFire>,
    )>();
    tokio::spawn({
      let stream = state.stream.clone();
      async move {
        loop {
          let (txn_records, committed_events, mut signals_ready_to_fire) =
            txn_receiver.recv().await.unwrap();
          // Write changes via journal.
          journal.commit(txn_records).await;
          // Add events.
          stream.write().sync(committed_events);
          // Fire signals.
          for s in signals_ready_to_fire.drain(..) {
            match s {
              SignalReadyToFire::Commit(s, v) => s.signal(v),
              SignalReadyToFire::Create(s, v) => s.signal(v),
              SignalReadyToFire::Delete(s, v) => s.signal(v),
            };
          }
        }
      }
    });

    let (action_sender, action_receiver) = crossbeam_channel::unbounded::<StateAction>();
    std::thread::spawn({
      move || {
        // These are outside the loop as we may not commit on an iteration (i.e. `!objects_pending_drop.is_empty()`) and so have not committed these yet.
        let mut signals_ready_to_fire = Vec::new();
        let mut txn = Transaction::new(state.cfg.spage_size_pow2);

        loop {
          // We must check in regularly in case there are uncommitted changes that were postponed due to `objects_pending_drop` not being empty.
          // TODO Tune timeout.
          let action = action_receiver.recv_timeout(Duration::from_micros(1));
          if let Ok(action) = action {
            action.execute(&mut state, &mut txn, &mut signals_ready_to_fire);
            // Avoid slack time: do as much work as possible.
            while let Ok(action) = action_receiver.try_recv() {
              action.execute(&mut state, &mut txn, &mut signals_ready_to_fire);
            }
          };
          // There's no work left at this very moment, so it's a perfect time to commit the change as one batch.
          // However, we may not be able to if we're still waiting for some AutoLifecycleObject references to drop.
          if !objects_pending_drop.is_empty() {
            continue;
          };
          // Commit state changes.
          state.incomplete_list.commit(&mut txn, &mut state.allocator);
          state.object_id_serial.commit(&mut txn);
          let committed_events = state.stream.write().commit(&mut txn);
          state.buckets.commit(&mut txn, &mut state.allocator);
          state.metrics.commit(&mut txn);
          // Commit allocator last as some other `.commit` calls require the allocator.
          state.allocator.commit(&mut txn);
          if txn.is_empty() {
            // We didn't make any changes to the state.
            assert!(signals_ready_to_fire.is_empty());
            continue;
          };
          txn_sender
            .send((
              txn.drain_all().collect(),
              committed_events,
              signals_ready_to_fire.drain(..).collect(),
            ))
            .unwrap();
        }
      }
    });

    Self { action_sender }
  }

  pub fn send_action(&self, action: StateAction) {
    self.action_sender.send(action).unwrap();
  }
}
