use crate::ctx::Ctx;
use crate::object::get_bundle_index_for_key;
use crate::object::load_bundle_from_device;
use crate::object::serialise_bundle;
use crate::object::ObjectTuple;
use crate::object::ObjectTupleData;
use crate::object::ObjectTupleKey;
use dashmap::DashMap;
use off64::u64;
use rustc_hash::FxHashMap;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tokio::spawn;
use tokio::time::Instant;

struct RetryRequestError;

type BundleTaskResult<T> = Result<T, RetryRequestError>;

enum BundleTask {
  Upsert {
    key: ObjectTupleKey,
    data: ObjectTupleData,
    if_not_exists: bool,
    signal: SignalFutureController<BundleTaskResult<()>>,
  },
  Read {
    key: ObjectTupleKey,
    signal: SignalFutureController<BundleTaskResult<Option<ObjectTupleData>>>,
  },
  Delete {
    key: ObjectTupleKey,
    signal: SignalFutureController<BundleTaskResult<()>>,
  },
}

struct Bundle {
  backlog: tokio::sync::mpsc::UnboundedSender<BundleTask>,
}

impl Bundle {
  fn new(ctx: Arc<Ctx>, bundle_idx: u64) -> Self {
    ctx
      .metrics
      .0
      .bundle_cache_miss
      .fetch_add(1, Ordering::Relaxed);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<BundleTask>();
    spawn({
      let ctx = ctx.clone();
      async move {
        let dev = &ctx.device;
        let pages = &ctx.pages;

        let load_start = Instant::now();
        // TODO OPTIMISATION: Avoid initial Vec allocation, build FxHashMap directly.
        let mut tuples: FxHashMap<_, _> = load_bundle_from_device(&dev, &pages, bundle_idx)
          .await
          .into_iter()
          .map(|t| (t.key, t.data))
          .collect();
        ctx
          .metrics
          .0
          .bundle_cache_load_us
          .fetch_add(u64!(load_start.elapsed().as_micros()), Ordering::Relaxed);
        let mut flush_signals = Vec::<SignalFutureController<BundleTaskResult<()>>>::new();
        // There isn't much point to making this parallel/concurrent:
        // - All statements except the `dev.write_at` is sync anyway, so cannot be made concurrent.
        // - The device write takes orders of magnitudes longer than the CPU time collecting and updating data structures, so the extra complexity, subtlety, verbosity, and locking would not be worth it.
        macro_rules! flush {
          () => {{
            let flush_start = Instant::now();
            // TODO Better error/panic message on overflow.
            // TODO Avoid clone.
            let new_bundle = serialise_bundle(
              pages,
              tuples.iter().map(|(key, data)| ObjectTuple {
                key: key.clone(),
                data: data.clone(),
              }),
            );
            dev
              .write_at(bundle_idx * pages.spage_size(), new_bundle)
              .await;
            // TODO Do we need `dev.sync()`? Could be expensive when run per-bundle (i.e. sector).
            for s in flush_signals.drain(..) {
              s.signal(Ok(()));
            }
            ctx
              .metrics
              .0
              .bundle_cache_flush_us
              .fetch_add(u64!(flush_start.elapsed().as_micros()), Ordering::Relaxed);
            ctx
              .metrics
              .0
              .bundle_cache_flush_count
              .fetch_add(1, Ordering::Relaxed);
          }};
        }
        // TODO Technically all tasks before loading has complete are cache misses.
        let mut seen_first = false;
        loop {
          // TODO Tune, configure hyperparameter. It affects delay before eviction and flush batch time.
          let res =
            tokio::time::timeout(std::time::Duration::from_micros(20), receiver.recv()).await;
          let Ok(res) = res else {
            if !flush_signals.is_empty() {
              // Elapsed timeout but we still have things to flush.
              flush!();
              continue;
            } else {
              ctx.metrics.0.bundle_cache_evict.fetch_add(1, Ordering::Relaxed);
              // Elapsed timeout and we have nothing to flush: we're too idle and should evict ourselves.
              // Remove from map first BEFORE dropping receiver, as otherwise we may still receive messages after we've drained it.
              ctx.bundles.map.remove(&bundle_idx).unwrap();
              // Stop receiving messages but don't lose any exisiting ones. This handles requests that have cloned this `Bundle` but haven't sent any message yet, so they'll fail because of this.
              receiver.close();
              // This handles requests that have cloned this `Bundle` AND sent a message, but we can no longer answer because we're no longer the authority.
              // This is correct; the official docs for `.close()` mentions this pattern, and it makes sense: the requester has either sent a message before it was closed (which this will handle), or tried to after (which would've failed), there is no in-between.
              while let Some(msg) = receiver.recv().await {
                match msg {
                  BundleTask::Upsert { signal, .. } => signal.signal(Err(RetryRequestError)),
                  BundleTask::Read { signal, .. } => signal.signal(Err(RetryRequestError)),
                  BundleTask::Delete { signal, .. } => signal.signal(Err(RetryRequestError)),
                }
              }
              drop(receiver);
              break;
            };
          };
          // The sender can never be dropped because we'll always have an entry in `bundle`.
          let t = res.unwrap();
          if !seen_first {
            seen_first = true;
          } else {
            ctx
              .metrics
              .0
              .bundle_cache_hit
              .fetch_add(1, Ordering::Relaxed);
          };
          match t {
            BundleTask::Upsert {
              data,
              key,
              if_not_exists,
              signal,
            } => {
              flush_signals.push(signal);
              let to_delete = match tuples.entry(key) {
                Entry::Occupied(mut o) => {
                  if if_not_exists {
                    None
                  } else {
                    Some(o.insert(data))
                  }
                }
                Entry::Vacant(v) => {
                  v.insert(data);
                  None
                }
              };
              if let Some(ObjectTupleData::Heap { size, dev_offset }) = to_delete {
                // TODO Add to a queue to release at a later time so we can be sure there's no readers who will read junk. Use a timer as a locking mechanism per object read/write/delete may be too much overhead.
                ctx.heap_allocator.lock().release(dev_offset, size);
                ctx
                  .metrics
                  .0
                  .heap_object_data_bytes
                  .fetch_sub(size.into(), Ordering::Relaxed);
              };
            }
            BundleTask::Read { key, signal } => {
              // We must flush first, as we cannot return uncommitted data as callers will assume that it's committed and safely persisted if returned from a read.
              // TODO OPTIMISATION: Only flush if specific key is dirty, not that any is dirty.
              if !flush_signals.is_empty() {
                flush!();
              };
              signal.signal(Ok(tuples.get(&key).cloned()));
            }
            BundleTask::Delete { key, signal } => {
              // We always need to signal, even if we don't delete anything, as otherwise it'll be stuck forever.
              flush_signals.push(signal);
              let deleted = tuples.remove(&key);
              if let Some(deleted) = deleted {
                ctx
                  .metrics
                  .0
                  .delete_op_count
                  .fetch_add(1, Ordering::Relaxed);
                ctx.metrics.0.object_count.fetch_sub(1, Ordering::Relaxed);
                if let ObjectTupleData::Heap { size, dev_offset } = deleted {
                  // TODO Add to a queue to release at a later time so we can be sure there's no readers who will read junk. Use a timer as a locking mechanism per object read/write/delete may be too much overhead.
                  ctx.heap_allocator.lock().release(dev_offset, size);
                  ctx
                    .metrics
                    .0
                    .heap_object_data_bytes
                    .fetch_sub(size.into(), Ordering::Relaxed);
                };
              };
            }
          };
        }
      }
    });
    Self { backlog: sender }
  }
}

#[derive(Clone)]
pub(crate) struct Bundles {
  map: Arc<DashMap<u64, Arc<Bundle>>>,
  bundle_count: u64,
}

impl Bundles {
  pub fn new(bundle_count: u64) -> Self {
    Self {
      map: Default::default(),
      bundle_count,
    }
  }

  pub async fn read_tuple(&self, ctx: Arc<Ctx>, k: TinyBuf) -> Option<ObjectTupleData> {
    let hash = blake3::hash(&k);
    let bundle_idx = get_bundle_index_for_key(&hash, self.bundle_count);
    let key = ObjectTupleKey::from_raw(k, hash);
    loop {
      // When we call `self.map.entry`, we have a lock on that entry.
      // Clone to avoid holding lock past this statement.
      let bundle = self
        .map
        .entry(bundle_idx)
        .or_insert_with(|| Arc::new(Bundle::new(ctx.clone(), bundle_idx)))
        .clone();
      let (fut, fut_ctl) = SignalFuture::new();
      // Subtle race condition: the bundle coroutine is self-evicting and has stopped receiving on the channel, but we were able to get it just before it removed itself from the map. This should be a very rare event and so should be performant.
      let Ok(_) = bundle.backlog.send(BundleTask::Read {
        key: key.clone(),
        signal: fut_ctl,
      }) else {
        // The queue has been closed, retry.
        continue;
      };
      let Ok(res) = fut.await else {
        // The queue has been closed, retry.
        continue;
      };
      return res;
    }
  }

  pub async fn upsert_tuple(
    &self,
    ctx: Arc<Ctx>,
    k: TinyBuf,
    data: ObjectTupleData,
    if_not_exists: bool,
  ) {
    let hash = blake3::hash(&k);
    let bundle_idx = get_bundle_index_for_key(&hash, self.bundle_count);
    let key = ObjectTupleKey::from_raw(k, hash);
    loop {
      // When we call `self.map.entry`, we have a lock on that entry.
      // Clone to avoid holding lock past this statement.
      let bundle = self
        .map
        .entry(bundle_idx)
        .or_insert_with(|| Arc::new(Bundle::new(ctx.clone(), bundle_idx)))
        .clone();
      let (fut, fut_ctl) = SignalFuture::new();
      // Subtle race condition: the bundle coroutine is self-evicting and has stopped receiving on the channel, but we were able to get it just before it removed itself from the map. This should be a very rare event and so should be performant.
      let Ok(_) = bundle.backlog.send(BundleTask::Upsert {
        key: key.clone(),
        data: data.clone(),
        if_not_exists,
        signal: fut_ctl,
      }) else {
        // The queue has been closed, retry.
        continue;
      };
      let Ok(()) = fut.await else {
        // The queue has been closed, retry.
        continue;
      };
      break;
    }
  }

  pub async fn delete_tuple(&self, ctx: Arc<Ctx>, k: TinyBuf) {
    let hash = blake3::hash(&k);
    let bundle_idx = get_bundle_index_for_key(&hash, self.bundle_count);
    let key = ObjectTupleKey::from_raw(k, hash);
    loop {
      // When we call `self.map.entry`, we have a lock on that entry.
      // Clone to avoid holding lock past this statement.
      let bundle = self
        .map
        .entry(bundle_idx)
        .or_insert_with(|| Arc::new(Bundle::new(ctx.clone(), bundle_idx)))
        .clone();
      let (fut, fut_ctl) = SignalFuture::new();
      // Subtle race condition: the bundle coroutine is self-evicting and has stopped receiving on the channel, but we were able to get it just before it removed itself from the map. This should be a very rare event and so should be performant.
      let Ok(_) = bundle.backlog.send(BundleTask::Delete {
        key: key.clone(),
        signal: fut_ctl,
      }) else {
        // The queue has been closed, retry.
        continue;
      };
      let Ok(()) = fut.await else {
        // The queue has been closed, retry.
        continue;
      };
      break;
    }
  }
}
