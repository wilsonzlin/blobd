use blobd_universal_client::direct::Direct;
use blobd_universal_client::kv::Kv;
use blobd_universal_client::lite::Lite;
use blobd_universal_client::BlobdProvider;
use blobd_universal_client::CommitObjectInput;
use blobd_universal_client::CreateObjectInput;
use blobd_universal_client::DeleteObjectInput;
use blobd_universal_client::IncompleteToken;
use blobd_universal_client::InitCfg;
use blobd_universal_client::InitCfgPartition;
use blobd_universal_client::InspectObjectInput;
use blobd_universal_client::ReadObjectInput;
use blobd_universal_client::WriteObjectInput;
use bytesize::ByteSize;
use futures::StreamExt;
use off64::int::create_u64_le;
use off64::u64;
use off64::usz;
use rand::thread_rng;
use rand::Rng;
use rand::RngCore;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use stochastic_queue::stochastic_channel;
use stochastic_queue::StochasticMpmcRecvError;
use strum_macros::Display;
use tinybuf::TinyBuf;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::Instant;
use tracing::info;
use tracing::trace;
use twox_hash::xxh3::hash64_with_seed;

/*

# Stochastic stress tester

Run this program with this env var for logging: RUST_LOG=<log level>,runtime=info,tokio=info
This will prevent the [Tokio Instrumentation](https://github.com/tokio-rs/console/tree/main/console-subscriber) events from being printed.
Use [tokio-console](https://github.com/tokio-rs/console#running-the-console) to debug Tokio tasks (e.g. deadlocks).

- We should not have to generate and/or store much data in memory, as that will hit performance.
- Inputs should vary in length and content.
- We want to vary tasks, not just sequentially create => write => commit => read, either horizontally (create all then write all then ...) or vertically (per object). In the future we could use more complex dynamic probabilities, but for now a random pick of possible tasks is good enough.

*/

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Deserialize)]
enum TargetType {
  Direct,
  Kv,
  Lite,
}

#[derive(Deserialize)]
struct ConfigPartition {
  path: PathBuf,
  offset: u64,
  len: u64,
}

#[derive(Deserialize)]
struct Config {
  target: TargetType,

  /// For the "lite" target, there must only be one partition and its offset must be zero.
  partitions: Vec<ConfigPartition>,

  // How many buckets to allocate. Only applicable for the "lite" target. Defaults to `objects * 2`.
  buckets: Option<u64>,

  /// Maximum object key length. Defaults to 480.
  object_key_len_max: Option<u64>,

  /// Objects to create.
  objects: u64,

  /// Maximum size of a created object.
  maximum_object_size: ByteSize,

  /// Concurrency level. Defaults to 64.
  concurrency: Option<u64>,

  /// Size of random bytes pool. Defaults to 1 GiB.
  pool_size: Option<ByteSize>,

  /// Lpage size. Defaults to 16 MiB.
  lpage_size: Option<ByteSize>,

  /// Spage size. Defaults to 512 bytes.
  spage_size: Option<ByteSize>,
}

#[derive(Clone)]
struct Pool {
  data: Arc<Vec<u8>>,
}

impl Pool {
  fn new(size: u64) -> Self {
    let mut data = vec![0u8; usize::try_from(size).unwrap()];
    thread_rng().fill_bytes(&mut data);
    Self {
      data: Arc::new(data),
    }
  }

  fn get(&self, offset: u64, len: u64) -> &[u8] {
    let start = usz!(offset);
    let end = usz!(offset + len);
    &self.data[start..end]
  }

  fn get_then_prefix(&self, offset: u64, len: u64, prefix: u64) -> TinyBuf {
    let mut out = create_u64_le(prefix).to_vec();
    out.extend_from_slice(self.get(offset, len));
    out.into()
  }
}

#[derive(Display)]
enum Task {
  Create {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
  },
  Write {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    incomplete_token: IncompleteToken,
    chunk_offset: u64,
  },
  Commit {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    incomplete_token: IncompleteToken,
  },
  Inspect {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    object_id: u64,
  },
  Read {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    chunk_offset: u64,
    object_id: u64,
  },
  Delete {
    key_prefix: u64,
    key_len: u64,
    key_offset: u64,
    object_id: u64,
  },
}

#[derive(Default)]
struct TaskProgress {
  create: AtomicU64,
  write: AtomicU64,
  commit: AtomicU64,
  inspect: AtomicU64,
  read: AtomicU64,
  delete: AtomicU64,
}

#[tokio::main]
async fn main() {
  #[cfg(feature = "instrumentation")]
  console_subscriber::init();
  #[cfg(not(feature = "instrumentation"))]
  tracing_subscriber::fmt::init();

  let cli: Config = serde_yaml::from_str(
    &fs::read_to_string(env::args().nth(1).expect("config file path argument"))
      .expect("read config file"),
  )
  .expect("parse config file");
  let object_count = cli.objects;
  let bucket_count = cli.buckets.unwrap_or(object_count * 2);
  let concurrency = cli.concurrency.unwrap_or(64);
  let lpage_size = cli.lpage_size.map(|s| s.as_u64()).unwrap_or(16_777_216);
  let maximum_object_size = cli.maximum_object_size.as_u64();
  let object_key_len_max = cli.object_key_len_max.unwrap_or(480);
  let pool_size = cli
    .pool_size
    .map(|s| s.as_u64())
    .unwrap_or(1024 * 1024 * 1024 * 1);
  let spage_size = cli.spage_size.map(|s| s.as_u64()).unwrap_or(512);

  let cfg = InitCfg {
    bucket_count,
    lpage_size,
    object_count,
    spage_size,
    partitions: cli
      .partitions
      .into_iter()
      .map(|p| InitCfgPartition {
        len: p.len,
        offset: p.offset,
        path: p.path,
      })
      .collect(),
  };
  let blobd: Arc<dyn BlobdProvider> = match cli.target {
    TargetType::Direct => Arc::new(Direct::start(cfg).await),
    TargetType::Kv => Arc::new(Kv::start(cfg).await),
    TargetType::Lite => Arc::new(Lite::start(cfg).await),
  };

  info!(
    "initialising pool of size {}, this may take a while",
    ByteSize(pool_size)
  );
  let pool = Pool::new(pool_size);
  info!("pool initialised");
  let key_len_seed = thread_rng().next_u64();
  let key_offset_seed = thread_rng().next_u64();
  let data_len_seed = thread_rng().next_u64();
  let data_offset_seed = thread_rng().next_u64();

  // Progress bars would look nice and fancy, but we are more likely to want logs/traces than in-flight animations, and a summary of performance metrics at the end will be enough. Using progress bars will clash with the logger.

  let started = Instant::now();
  let completed_by_type = Arc::new(TaskProgress::default());
  let complete = Arc::new(AtomicBool::new(false));

  let (tasks_sender, tasks_receiver) = stochastic_channel::<Task>();
  let total_data_bytes = spawn_blocking({
    let tasks_sender = tasks_sender.clone();
    move || {
      info!(
        object_count = object_count,
        maximum_object_size = ByteSize(maximum_object_size).to_string(),
        "objects"
      );
      let mut total_data_bytes = 0;
      for i in 0..object_count {
        let key_len = (hash64_with_seed(&i.to_be_bytes(), key_len_seed)
          % u64::from(object_key_len_max - 1))
          + 1;
        let key_offset =
          hash64_with_seed(&i.to_be_bytes(), key_offset_seed) % (pool_size - key_len);
        let data_len =
          (hash64_with_seed(&i.to_be_bytes(), data_len_seed) % (maximum_object_size - 1)) + 1;
        let data_offset =
          hash64_with_seed(&i.to_be_bytes(), data_offset_seed) % (pool_size - data_len);
        total_data_bytes += data_len;
        tasks_sender
          .send(Task::Create {
            // We use a prefix as some random keys will be short enough that there will be conflicts.
            key_prefix: i,
            key_len,
            key_offset,
            data_len,
            data_offset,
          })
          .unwrap();
      }
      info!("sender complete");
      total_data_bytes
    }
  })
  .await
  .unwrap();

  // Background loop to regularly print out progress.
  spawn({
    let complete = complete.clone();
    let task_progress = completed_by_type.clone();
    async move {
      while !complete.load(Ordering::Relaxed) {
        sleep(Duration::from_secs(10)).await;
        info!(
          create = task_progress.create.load(Ordering::Relaxed),
          write = task_progress.write.load(Ordering::Relaxed),
          commit = task_progress.commit.load(Ordering::Relaxed),
          inspect = task_progress.inspect.load(Ordering::Relaxed),
          read = task_progress.read.load(Ordering::Relaxed),
          delete = task_progress.delete.load(Ordering::Relaxed),
          "progress"
        );
      }
    }
  });

  let mut workers = Vec::new();
  for worker_no in 0..concurrency {
    let blobd = blobd.clone();
    let pool = pool.clone();
    let complete = complete.clone();
    let completed_by_type = completed_by_type.clone();
    let tasks_sender = tasks_sender.clone();
    let tasks_receiver = tasks_receiver.clone();
    workers.push(spawn(async move {
      while !complete.load(Ordering::Relaxed) {
        // We must use a timeout and regularly check the completion count, as we hold a sender so the channel won't naturally end.
        // WARNING: We cannot use `recv_timeout` as it's blocking.
        let t = match tasks_receiver.try_recv() {
          Ok(Some(t)) => t,
          Err(StochasticMpmcRecvError::NoSenders) => break,
          Ok(None) => {
            // Keep this timeout small so that total execution time is accurate.
            sleep(Duration::from_millis(100)).await;
            continue;
          }
        };
        trace!(worker_no, task_type = t.to_string(), "received task");
        match t {
          Task::Create {
            key_prefix,
            key_len,
            key_offset,
            data_len,
            data_offset,
          } => {
            let res = blobd
              .create_object(CreateObjectInput {
                key: pool.get_then_prefix(key_offset, key_len, key_prefix),
                size: data_len,
              })
              .await;
            completed_by_type.create.fetch_add(1, Ordering::Relaxed);
            tasks_sender
              .send(Task::Write {
                key_prefix,
                key_len,
                key_offset,
                data_len,
                data_offset,
                incomplete_token: res.token,
                chunk_offset: 0,
              })
              .unwrap();
          }
          Task::Write {
            key_prefix,
            key_len,
            key_offset,
            data_len,
            data_offset,
            chunk_offset,
            incomplete_token,
          } => {
            let next_chunk_offset = chunk_offset + lpage_size;
            let chunk_len = if next_chunk_offset <= data_len {
              lpage_size
            } else {
              data_len - chunk_offset
            };
            blobd
              .write_object(WriteObjectInput {
                key: pool.get_then_prefix(key_offset, key_len, key_prefix),
                data: pool.get(data_offset + chunk_offset, chunk_len),
                incomplete_token: incomplete_token.clone(),
                offset: chunk_offset,
              })
              .await;
            tasks_sender
              .send(if next_chunk_offset < data_len {
                Task::Write {
                  key_prefix,
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  incomplete_token,
                  chunk_offset: next_chunk_offset,
                }
              } else {
                completed_by_type.write.fetch_add(1, Ordering::Relaxed);
                Task::Commit {
                  key_prefix,
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  incomplete_token,
                }
              })
              .unwrap();
          }
          Task::Commit {
            key_prefix,
            key_len,
            key_offset,
            data_len,
            data_offset,
            incomplete_token,
          } => {
            let res = blobd
              .commit_object(CommitObjectInput { incomplete_token })
              .await;
            completed_by_type.commit.fetch_add(1, Ordering::Relaxed);
            tasks_sender
              .send(Task::Inspect {
                key_prefix,
                key_len,
                key_offset,
                data_len,
                data_offset,
                object_id: res.object_id,
              })
              .unwrap();
          }
          Task::Inspect {
            key_prefix,
            key_len,
            key_offset,
            data_len,
            data_offset,
            object_id,
          } => {
            let res = blobd
              .inspect_object(InspectObjectInput {
                key: pool.get_then_prefix(key_offset, key_len, key_prefix),
                id: Some(object_id),
              })
              .await;
            completed_by_type.inspect.fetch_add(1, Ordering::Relaxed);
            assert_eq!(res.id, object_id);
            assert_eq!(res.size, data_len);
            tasks_sender
              .send(Task::Read {
                key_prefix,
                key_len,
                key_offset,
                data_len,
                data_offset,
                chunk_offset: 0,
                object_id,
              })
              .unwrap();
          }
          Task::Read {
            key_prefix,
            key_len,
            key_offset,
            data_len,
            data_offset,
            mut chunk_offset,
            object_id,
          } => {
            // Read a random amount to test various cases stochastically.
            let end = thread_rng().gen_range(chunk_offset + 1..=data_len);
            let mut res = blobd
              .read_object(ReadObjectInput {
                end: Some(end),
                start: chunk_offset,
                key: pool.get_then_prefix(key_offset, key_len, key_prefix),
                stream_buffer_size: 1024 * 16,
                id: Some(object_id),
              })
              .await;
            while let Some(chunk) = res.data_stream.next().await {
              let chunk_len = u64!(chunk.len());
              // Don't use assert_eq! as it will print a lot of raw bytes.
              assert!(chunk == pool.get(data_offset + chunk_offset, chunk_len));
              chunk_offset += chunk_len;
              assert!(chunk_offset <= end);
            }
            assert_eq!(chunk_offset, end);
            if end < data_len {
              tasks_sender
                .send(Task::Read {
                  key_prefix,
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  chunk_offset,
                  object_id,
                })
                .unwrap();
            } else {
              completed_by_type.read.fetch_add(1, Ordering::Relaxed);
              tasks_sender
                .send(Task::Delete {
                  key_prefix,
                  key_len,
                  key_offset,
                  object_id,
                })
                .unwrap();
            };
          }
          Task::Delete {
            key_prefix,
            key_len,
            key_offset,
            object_id,
          } => {
            blobd
              .delete_object(DeleteObjectInput {
                key: pool.get_then_prefix(key_offset, key_len, key_prefix),
                id: Some(object_id),
              })
              .await;
            if completed_by_type.delete.fetch_add(1, Ordering::Relaxed) + 1 == object_count {
              complete.store(true, Ordering::Relaxed);
            }
          }
        };
      }
    }));
  }
  drop(tasks_sender);
  drop(tasks_receiver);
  for t in workers {
    t.await.unwrap();
  }

  // TODO For libblobd-lite: assert all tiles are solid, no fragmented tiles.

  let final_metrics = blobd.metrics();
  for (key, value) in final_metrics {
    info!(key, value, "final metric");
  }

  let exec_sec = started.elapsed().as_secs_f64();
  info!(
    execution_seconds = exec_sec,
    total_data_bytes,
    data_mib_written_per_sec = (total_data_bytes as f64) / exec_sec / 1024.0 / 1024.0,
    objects_processed_per_sec = (object_count as f64) / exec_sec,
    "all done"
  );
}
