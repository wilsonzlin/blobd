pub mod action;

use self::action::commit_object::action_commit_object;
use self::action::commit_object::ActionCommitObjectInput;
use self::action::create_object::action_create_object;
use self::action::create_object::ActionCreateObjectInput;
use self::action::delete_object::action_delete_object;
use self::action::delete_object::ActionDeleteObjectInput;
use crate::allocator::Allocator;
use crate::journal::Journal;
use crate::journal::Transaction;
use crate::metrics::BlobdMetrics;
use crate::object::id::ObjectIdSerial;
use crate::objects::CommittedObjects;
use crate::objects::IncompleteObjects;
use crate::op::commit_object::OpCommitObjectOutput;
use crate::op::create_object::OpCreateObjectOutput;
use crate::op::delete_object::OpDeleteObjectOutput;
use crate::op::OpResult;
use crate::pages::Pages;
use crate::stream::Stream;
use crate::stream::StreamEventId;
use bufpool::buf::Buf;
use signal_future::SignalFutureController;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::info_span;
use tracing::Instrument;

// We must lock these together instead of individually. Inside a transaction, it will make mutation calls to these subsystems, and transactions get committed in the order they started. However, it's possible during the transaction that the earliest transaction does not reach all subsystems first, which would mean that the changes for some subsystems may get written out of order. For example, consider that request 1 may update incomplete list before request 0, even though request 0 came first, created an earlier transaction, and returned from its call to the free list before request 1, purely because of unfortunate luck with lock acquisition or the Tokio or Linux thread scheduler. (A simpler example would be if request 0 updates incomplete list first then allocator second, while request 1 updates allocator first then incomplete list second.) Request 0's transaction is always committed before request 1's (enforced by WriteJournal), but request 0 contains changes to incomplete list that depend on request 1's changes, so writing request 1 will clobber request 0's changes and corrupt the state.
pub(crate) struct State {
  pub committed_objects: CommittedObjects,
  pub data_allocator: Allocator,
  pub incomplete_objects: IncompleteObjects,
  pub metadata_allocator: Allocator,
  pub metrics: Arc<BlobdMetrics>,
  pub object_id_serial: ObjectIdSerial,
  pub pages: Pages,
  pub partition_idx: usize,
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
      StateAction::Commit(i, s) => {
        SignalReadyToFire::Commit(s, action_commit_object(state, txn, i))
      }
      StateAction::Create(i, s) => {
        SignalReadyToFire::Create(s, action_create_object(state, txn, i))
      }
      StateAction::Delete(i, s) => {
        SignalReadyToFire::Delete(s, action_delete_object(state, txn, i))
      }
    });
  }
}

impl StateWorker {
  pub fn start(mut state: State, journal: Journal) -> Self {
    let (txn_sender, mut txn_receiver) = tokio::sync::mpsc::unbounded_channel::<(
      Vec<(u64, Buf)>,
      Vec<StreamEventId>,
      Vec<SignalReadyToFire>,
    )>();
    tokio::spawn({
      let stream = state.stream.clone();
      let span = info_span!("journal committer", partition_index = state.partition_idx);
      async move {
        loop {
          let (txn_records, pending_events, mut signals_ready_to_fire) =
            txn_receiver.recv().await.unwrap();
          // Write changes via journal.
          journal.commit(txn_records).await;
          // Add events.
          stream.write().make_available(&pending_events);
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
      .instrument(span)
    });

    let (action_sender, action_receiver) = crossbeam_channel::unbounded::<StateAction>();
    std::thread::spawn({
      move || {
        let _span = info_span!("action worker", partition_index = state.partition_idx).entered();
        // These are outside the loop as we may not commit on an iteration (i.e. `!objects_pending_drop.is_empty()`) and so have not committed these yet.
        let mut signals_ready_to_fire = Vec::new();
        let mut txn = Transaction::new(state.pages.spage_size_pow2);

        loop {
          let action = action_receiver.recv().unwrap();
          action.execute(&mut state, &mut txn, &mut signals_ready_to_fire);
          // Avoid slack time: do as much work as possible.
          while let Ok(action) = action_receiver.try_recv() {
            action.execute(&mut state, &mut txn, &mut signals_ready_to_fire);
          }
          // There's no work left at this very moment, so it's a perfect time to commit the change as one batch.
          // Commit state changes.
          state.object_id_serial.commit(&mut txn);
          let pending_events = state.stream.write().commit(&mut txn);
          if txn.is_empty() {
            // We didn't make any changes to the state.
            assert!(signals_ready_to_fire.is_empty());
            continue;
          };
          txn_sender
            .send((
              txn.drain_all().collect(),
              pending_events,
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
