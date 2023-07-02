use blobd_universal_client::direct::Direct;
use blobd_universal_client::kv::Kv;
use blobd_universal_client::lite::Lite;
use blobd_universal_client::BlobdProvider;
use blobd_universal_client::CommitObjectInput;
use blobd_universal_client::CreateObjectInput;
use blobd_universal_client::DeleteObjectInput;
use blobd_universal_client::InitCfg;
use blobd_universal_client::InitCfgPartition;
use blobd_universal_client::InspectObjectInput;
use blobd_universal_client::ReadObjectInput;
use blobd_universal_client::WriteObjectInput;
use bytesize::ByteSize;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::create_u64_be;
use off64::usz;
use serde::Deserialize;
use std::cmp::min;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::info;

/*

# Benchmarker

This is both similar and almost the opposite of the stochastic stress tester: this is designed to try and find the maximum possible performance, leveraging any advantage possible. When reviewing the output results, keep in mind:

- The reads may be extremely fast, because the writes caused all the mmap pages to be cached.
- The compiler may be optimising away reads because the returned data isn't being used.
- It's not a realistic workload:
  - All ops of the same type are performed at once instead of being interspersed.
  - Objects are created, written, committed, inspected, read, and deleted in the exact same order for every op.
  - The object size is identical for all objects.
  - The data being written/read is full of zeros.
- No correctness checks are done.

Despite these limitations, the benchmarker can be useful to find hotspots and slow code (when profiling), high-level op performance, and upper limit of possible performance.

*/

const EMPTY_POOL: [u8; 16_777_216] = [0u8; 16_777_216];

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
#[serde(deny_unknown_fields)]
struct Config {
  target: TargetType,

  /// For the "lite" target, there must only be one partition and its offset must be zero.
  partitions: Vec<ConfigPartition>,

  // How many buckets to allocate. Only applicable for the "lite" target. Defaults to `objects * 2`.
  buckets: Option<u64>,

  /// Objects to create.
  objects: u64,

  /// Object size.
  object_size: ByteSize,

  /// Read size. Defaults to 4 MiB.
  read_size: Option<ByteSize>,

  /// Read stream buffer size. Defaults to 16 KiB.
  read_stream_buffer_size: Option<ByteSize>,

  /// Concurrency level. Defaults to 64.
  concurrency: Option<usize>,

  /// Lpage size. Defaults to 16 MiB.
  lpage_size: Option<ByteSize>,

  /// Spage size. Defaults to 512 bytes.
  spage_size: Option<ByteSize>,

  /// Only applies to Kv target. Defaults to 1 GiB.
  log_buffer_size: Option<ByteSize>,
}

#[tokio::main]
async fn main() {
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
  let object_size = cli.object_size.as_u64();
  let read_size = cli.read_size.map(|s| s.as_u64()).unwrap_or(1024 * 1024 * 4);
  let read_stream_buffer_size = cli
    .read_stream_buffer_size
    .map(|s| s.as_u64())
    .unwrap_or(1024 * 16);
  let spage_size = cli.spage_size.map(|s| s.as_u64()).unwrap_or(512);

  let init_cfg = InitCfg {
    bucket_count,
    log_buffer_size: cli
      .log_buffer_size
      .map(|s| s.as_u64())
      .unwrap_or(1024 * 1024 * 1024),
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
    TargetType::Direct => Arc::new(Direct::start(init_cfg).await),
    TargetType::Kv => Arc::new(Kv::start(init_cfg).await),
    TargetType::Lite => Arc::new(Lite::start(init_cfg).await),
  };

  let now = Instant::now();
  let incomplete_tokens = Arc::new(parking_lot::Mutex::new(Vec::new()));
  iter(0..object_count)
    .for_each_concurrent(Some(concurrency), |i| {
      let blobd = blobd.clone();
      let incomplete_tokens = incomplete_tokens.clone();
      async move {
        let res = blobd
          .create_object(CreateObjectInput {
            key: create_u64_be(i).into(),
            size: object_size,
          })
          .await;
        incomplete_tokens.lock().push((i, res.token));
      }
    })
    .await;
  let create_exec_secs = now.elapsed().as_secs_f64();
  info!(
    create_exec_secs,
    create_ops_per_second = (object_count as f64) / create_exec_secs,
    "completed all create ops",
  );

  let now = Instant::now();
  iter(incomplete_tokens.lock().as_slice())
    .for_each_concurrent(Some(concurrency), |(key, incomplete_token)| {
      let blobd = blobd.clone();
      async move {
        for offset in (0..object_size).step_by(usz!(lpage_size)) {
          let data_len = min(object_size - offset, lpage_size);
          blobd
            .write_object(WriteObjectInput {
              key: create_u64_be(*key).into(),
              offset,
              incomplete_token: incomplete_token.clone(),
              data: &EMPTY_POOL[..usz!(data_len)],
            })
            .await;
        }
      }
    })
    .await;
  let write_exec_secs = now.elapsed().as_secs_f64();
  info!(
    write_exec_secs,
    write_ops_per_second = object_count as f64 / write_exec_secs,
    write_mib_per_second = (object_count * object_size) as f64 / write_exec_secs / 1024.0 / 1024.0,
    "completed all write ops",
  );

  let now = Instant::now();
  iter(incomplete_tokens.lock().as_slice())
    .for_each_concurrent(Some(concurrency), |(_, incomplete_token)| {
      let blobd = blobd.clone();
      async move {
        blobd
          .commit_object(CommitObjectInput {
            incomplete_token: incomplete_token.clone(),
          })
          .await;
      }
    })
    .await;
  let commit_exec_secs = now.elapsed().as_secs_f64();
  info!(
    commit_exec_secs,
    commit_ops_per_second = (object_count as f64) / commit_exec_secs,
    "completed all commit ops",
  );

  let now = Instant::now();
  iter(0..object_count)
    .for_each_concurrent(Some(concurrency), |i| {
      let blobd = blobd.clone();
      async move {
        blobd
          .inspect_object(InspectObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
      }
    })
    .await;
  let inspect_exec_secs = now.elapsed().as_secs_f64();
  info!(
    inspect_exec_secs,
    inspect_ops_per_second = (object_count as f64) / inspect_exec_secs,
    "completed all inspect ops",
  );

  let now = Instant::now();
  iter(0..object_count)
    .for_each_concurrent(Some(concurrency), |i| {
      let blobd = blobd.clone();
      async move {
        for start in (0..object_size).step_by(usz!(read_size)) {
          let res = blobd
            .read_object(ReadObjectInput {
              key: create_u64_be(i).into(),
              id: None,
              start,
              end: Some(min(object_size, start + read_size)),
              stream_buffer_size: read_stream_buffer_size,
            })
            .await;
          let page_count = res.data_stream.count().await;
          // Do something with `page_count` so that the compiler doesn't just drop it, and then possibly drop the stream too.
          assert!(page_count > 0);
        }
      }
    })
    .await;
  let read_exec_secs = now.elapsed().as_secs_f64();
  info!(
    read_exec_secs,
    read_ops_per_second = object_count as f64 / read_exec_secs,
    read_mib_per_second = (object_count * object_size) as f64 / read_exec_secs / 1024.0 / 1024.0,
    "completed all read ops",
  );

  let now = Instant::now();
  iter(0..object_count)
    .for_each_concurrent(Some(concurrency), |i| {
      let blobd = blobd.clone();
      async move {
        blobd
          .delete_object(DeleteObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
      }
    })
    .await;
  let delete_exec_secs = now.elapsed().as_secs_f64();
  info!(
    delete_exec_secs,
    delete_ops_per_second = (object_count as f64) / delete_exec_secs,
    "completed all delete ops",
  );

  let final_metrics = blobd.metrics();
  for (key, value) in final_metrics {
    info!(key, value, "final metric");
  }

  info!("all done");
}
