use clap::Parser;
use futures::stream::once;
use futures::StreamExt;
use libblobd::incomplete_token::IncompleteToken;
use libblobd::object::OBJECT_KEY_LEN_MAX;
use libblobd::op::commit_object::OpCommitObjectInput;
use libblobd::op::create_object::OpCreateObjectInput;
use libblobd::op::delete_object::OpDeleteObjectInput;
use libblobd::op::inspect_object::OpInspectObjectInput;
use libblobd::op::read_object::OpReadObjectInput;
use libblobd::op::write_object::OpWriteObjectInput;
use libblobd::BlobdCfg;
use libblobd::BlobdLoader;
use off64::u64;
use off64::u8;
use off64::usz;
use rand::thread_rng;
use rand::Rng;
use rand::RngCore;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use stochastic_queue::stochastic_channel;
use stochastic_queue::StochasticMpmcRecvTimeoutError;
use strum_macros::Display;
use tokio::join;
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::Instant;
use tracing::info;
use tracing::trace;
use tracing::warn;
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

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
  /// Path to the device or regular file to use as the underlying storage.
  #[arg(long)]
  device: PathBuf,

  // How many buckets to allocate. Defaults to 131,072.
  #[arg(long, default_value_t = 131_072)]
  buckets: u64,

  /// Objects to create. Defaults to 100,000.
  #[arg(long, default_value_t = 100_000)]
  objects: u64,

  /// Maximum size of a created object. Defaults to 150 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 150)]
  maximum_object_size: u64,

  /// Concurrency level. Defaults to 64.
  #[arg(long, default_value_t = 64)]
  threads: u64,

  /// Size of random bytes pool. Defaults to 1 GiB.
  #[arg(long, default_value_t = 1024 * 1024 * 1024)]
  pool_size: u64,

  /// Lpage size. Defaults to 16 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 16)]
  lpage_size: u64,

  /// Spage size. Defaults to 512 bytes.
  #[arg(long, default_value_t = 512)]
  spage_size: u64,
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

  fn get(&self, offset: u64, len: u64) -> Vec<u8> {
    let start = usz!(offset);
    let end = usz!(offset + len);
    self.data[start..end].to_vec()
  }
}

#[derive(Clone, PartialEq, Eq, Display)]
enum Task {
  Create {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
  },
  Write {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    incomplete_token: IncompleteToken,
    chunk_offset: u64,
  },
  Commit {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    incomplete_token: IncompleteToken,
  },
  Inspect {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    object_id: u64,
  },
  Read {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    chunk_offset: u64,
    object_id: u64,
  },
  Delete {
    key_len: u64,
    key_offset: u64,
    object_id: u64,
  },
}

#[tokio::main]
async fn main() {
  #[cfg(feature = "instrumentation")]
  console_subscriber::init();
  #[cfg(not(feature = "instrumentation"))]
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  assert!(cli.buckets.is_power_of_two());
  let bucket_count_log2: u8 = cli.buckets.ilog2().try_into().unwrap();

  let device_size = get_file_len_via_seek(&cli.device).await.unwrap();

  let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());
  let device = SeekableAsyncFile::open(
    &cli.device,
    device_size,
    io_metrics,
    Duration::from_micros(200),
    0,
  )
  .await;

  let blobd = BlobdLoader::new(device.clone(), device_size, BlobdCfg {
    bucket_count_log2,
    bucket_lock_count_log2: bucket_count_log2,
    reap_objects_after_secs: 60 * 60 * 24 * 7,
    lpage_size_pow2: u8!(cli.lpage_size.ilog2()),
    spage_size_pow2: u8!(cli.spage_size.ilog2()),
    // We must enable versioning as some objects will have duplicate keys, and then their derived tasks won't work unless they were the last to commit.
    versioning: true,
  });
  blobd.format().await;
  info!("formatted device");
  let blobd = blobd.load().await;
  info!("loaded device");

  spawn({
    let blobd = blobd.clone();
    let device = device.clone();
    async move {
      join! {
        blobd.start(),
        device.start_delayed_data_sync_background_loop(),
      };
    }
  });

  info!(
    "initialising pool of size {} MiB, this may take a while",
    (cli.pool_size as f64) / 1024.0 / 1024.0
  );
  let pool = Pool::new(cli.pool_size);
  info!("pool initialised");
  let key_len_seed = thread_rng().next_u64();
  let key_offset_seed = thread_rng().next_u64();
  let data_len_seed = thread_rng().next_u64();
  let data_offset_seed = thread_rng().next_u64();

  // Progress bars would look nice and fancy, but we are more likely to want logs/traces than in-flight animations, and a summary of performance metrics at the end will be enough. Using progress bars will clash with the logger.

  let started = Instant::now();
  let completed = Arc::new(AtomicU64::new(0));

  let (tasks_sender, tasks_receiver) = stochastic_channel::<Task>();
  let total_data_bytes = spawn_blocking({
    let tasks_sender = tasks_sender.clone();
    move || {
      info!(
        object_count = cli.objects,
        maximum_object_size = cli.maximum_object_size,
        "objects"
      );
      let mut total_data_bytes = 0;
      for i in 0..cli.objects {
        let key_len = (hash64_with_seed(&i.to_be_bytes(), key_len_seed)
          % u64::from(OBJECT_KEY_LEN_MAX - 1))
          + 1;
        let key_offset =
          hash64_with_seed(&i.to_be_bytes(), key_offset_seed) % (cli.pool_size - key_len);
        let data_len =
          (hash64_with_seed(&i.to_be_bytes(), data_len_seed) % (cli.maximum_object_size - 1)) + 1;
        let data_offset =
          hash64_with_seed(&i.to_be_bytes(), data_offset_seed) % (cli.pool_size - data_len);
        total_data_bytes += data_len;
        tasks_sender
          .send(Task::Create {
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
  spawn({
    let completed = completed.clone();
    async move {
      loop {
        sleep(Duration::from_secs(10)).await;
        let completed = completed.load(Ordering::Relaxed);
        if completed == cli.objects {
          break;
        };
        info!(completed, "progress");
      }
    }
  });
  let mut threads = Vec::new();
  for thread_no in 0..cli.threads {
    let blobd = blobd.clone();
    let pool = pool.clone();
    let completed = completed.clone();
    let tasks_sender = tasks_sender.clone();
    let tasks_receiver = tasks_receiver.clone();
    threads.push(spawn(async move {
      let cur_task: Arc<RwLock<Option<(Instant, Task)>>> = Default::default();
      spawn({
        let completed = completed.clone();
        let cur_task = cur_task.clone();
        let mut last_seen: Option<(Instant, Task)> = None;
        async move {
          loop {
            sleep(Duration::from_secs(10)).await;
            if completed.load(Ordering::Relaxed) == cli.objects {
              break;
            };
            let cur = cur_task.read().await.clone();
            if cur != last_seen {
              last_seen = cur;
            } else if let Some((last_time, t)) = &last_seen {
              warn!(
                task = t.to_string(),
                duration_sec = last_time.elapsed().as_secs(),
                "still processing task"
              );
            };
          }
        }
      });
      loop {
        if completed.load(Ordering::Relaxed) == cli.objects {
          break;
        };
        // We must use a timeout and regularly check the completion count, as we hold a sender so the channel won't naturally end.
        let t = match tasks_receiver.recv_timeout(Duration::from_secs(1)) {
          Ok(t) => t,
          Err(StochasticMpmcRecvTimeoutError::NoSenders) => break,
          Err(StochasticMpmcRecvTimeoutError::Timeout) => {
            trace!(thread_no, "still waiting for task");
            continue;
          }
        };
        *cur_task.write().await = Some((Instant::now(), t.clone()));
        trace!(thread_no, task_type = t.to_string(), "received task");
        match t {
          Task::Create {
            key_len,
            key_offset,
            data_len,
            data_offset,
          } => {
            let res = blobd
              .create_object(OpCreateObjectInput {
                key: pool.get(key_offset, key_len),
                size: data_len,
                assoc_data: Vec::new(),
              })
              .await
              .unwrap();
            tasks_sender
              .send(Task::Write {
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
            key_len,
            key_offset,
            data_len,
            data_offset,
            chunk_offset,
            incomplete_token,
          } => {
            let next_chunk_offset = chunk_offset + cli.lpage_size;
            let chunk_len = if next_chunk_offset <= data_len {
              cli.lpage_size
            } else {
              data_len - chunk_offset
            };
            blobd
              .write_object(OpWriteObjectInput {
                data_len: chunk_len,
                data_stream: once(async { Ok(pool.get(data_offset + chunk_offset, chunk_len)) })
                  .boxed(),
                incomplete_token,
                offset: chunk_offset,
              })
              .await
              .unwrap();
            tasks_sender
              .send(if next_chunk_offset < data_len {
                Task::Write {
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  incomplete_token,
                  chunk_offset: next_chunk_offset,
                }
              } else {
                Task::Commit {
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
            key_len,
            key_offset,
            data_len,
            data_offset,
            incomplete_token,
          } => {
            let res = blobd
              .commit_object(OpCommitObjectInput { incomplete_token })
              .await
              .unwrap();
            tasks_sender
              .send(Task::Inspect {
                key_len,
                key_offset,
                data_len,
                data_offset,
                object_id: res.object_id,
              })
              .unwrap();
          }
          Task::Inspect {
            key_len,
            key_offset,
            data_len,
            data_offset,
            object_id,
          } => {
            let res = blobd
              .inspect_object(OpInspectObjectInput {
                key: pool.get(key_offset, key_len),
                id: Some(object_id),
              })
              .await
              .unwrap();
            assert_eq!(res.id, object_id);
            assert_eq!(res.size, data_len);
            tasks_sender
              .send(Task::Read {
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
              .read_object(OpReadObjectInput {
                end: Some(end),
                start: chunk_offset,
                key: pool.get(key_offset, key_len),
                stream_buffer_size: 1024 * 16,
                id: Some(object_id),
              })
              .await
              .unwrap();
            while let Some(chunk) = res.data_stream.next().await {
              let chunk = chunk.unwrap();
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
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  chunk_offset,
                  object_id,
                })
                .unwrap();
            } else {
              tasks_sender
                .send(Task::Delete {
                  key_len,
                  key_offset,
                  object_id,
                })
                .unwrap();
            };
          }
          Task::Delete {
            key_len,
            key_offset,
            object_id,
          } => {
            blobd
              .delete_object(OpDeleteObjectInput {
                key: pool.get(key_offset, key_len),
                id: Some(object_id),
              })
              .await
              .unwrap();
            completed.fetch_add(1, Ordering::Relaxed);
          }
        };
      }
      info!(thread_no, "thread complete");
    }));
  }
  drop(tasks_sender);
  drop(tasks_receiver);
  for t in threads {
    t.await.unwrap();
  }

  // TODO Assert all tiles are solid, no fragmented files.

  let exec_sec = started.elapsed().as_secs_f64();
  info!(
    execution_seconds = exec_sec,
    total_data_bytes,
    data_mib_written_per_sec = (total_data_bytes as f64) / exec_sec / 1024.0 / 1024.0,
    objects_processed_per_sec = (cli.objects as f64) / exec_sec,
    "all done"
  );
}
