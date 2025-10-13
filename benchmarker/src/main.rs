use ahash::HashMap;
use ahash::HashMapExt;
use bytesize::ByteSize;
use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::create_u64_be;
use off64::usz;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::min;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use store::direct::BlobdDirectStore;
use store::fs::FileSystemStore;
use store::kv::BlobdKVStore;
use store::lite::BlobdLiteStore;
use store::s3::S3StoreConfig;
use store::CommitObjectInput;
use store::CreateObjectInput;
use store::DeleteObjectInput;
use store::InitCfg;
use store::InitCfgPartition;
use store::InspectObjectInput;
use store::ReadObjectInput;
use store::Store;
use store::WriteObjectInput;
use tokio::spawn;
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
  - The object key is always exactly 8 bytes for all objects.
  - The object size is identical for all objects.
  - The data being written/read is full of zeros.
- No correctness checks are done.

Despite these limitations, the benchmarker can be useful to find hotspots and slow code (when profiling), high-level op performance, and upper limit of possible performance.

*/

const EMPTY_POOL: [u8; 16_777_216] = [0u8; 16_777_216];

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Deserialize, Serialize)]
enum TargetType {
  Direct,
  KV,
  Lite,
  FS,
  S3,
}

#[derive(Clone, Deserialize, Serialize)]
struct ConfigPartition {
  path: PathBuf,
  offset: u64,
  len: u64,
}

fn default_read_size() -> ByteSize {
  ByteSize::mib(4)
}

fn default_read_stream_buffer_size() -> ByteSize {
  ByteSize::kib(16)
}

fn default_concurrency() -> usize {
  64
}

fn default_lpage_size() -> ByteSize {
  ByteSize::mib(16)
}

fn default_spage_size() -> ByteSize {
  ByteSize::b(512)
}

fn default_log_buffer_size() -> ByteSize {
  ByteSize::gib(1)
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
  target: TargetType,

  /// Only applicable for the "fs" target.
  prefix: Option<PathBuf>,

  /// Only applicable for the "fs" target.
  tiering: Option<usize>,

  /// Only applicable for the "s3" target.
  s3: Option<S3StoreConfig>,

  /// For the "lite" target, there must only be one partition and its offset must be zero.
  #[serde(default)]
  partitions: Vec<ConfigPartition>,

  // How many buckets to allocate. Only applicable for the "lite" target.
  buckets: Option<u64>,

  /// Objects to create.
  objects: u64,

  /// Object size.
  object_size: ByteSize,

  /// Read size. Defaults to 4 MiB.
  #[serde(default = "default_read_size")]
  read_size: ByteSize,

  /// Read stream buffer size. Defaults to 16 KiB.
  #[serde(default = "default_read_stream_buffer_size")]
  read_stream_buffer_size: ByteSize,

  /// Lpage size. Defaults to 16 MiB.
  #[serde(default = "default_lpage_size")]
  lpage_size: ByteSize,

  /// Spage size. Defaults to 512 bytes.
  #[serde(default = "default_spage_size")]
  spage_size: ByteSize,

  /// Only applies to Kv target. Defaults to 1 GiB.
  #[serde(default = "default_log_buffer_size")]
  log_buffer_size: ByteSize,
}

#[derive(Parser)]
struct Cli {
  /// Path to the config file
  config: PathBuf,

  /// Skips formatting the device.
  #[arg(long)]
  skip_device_format: bool,

  /// Skips creating, writing, and committing objects. Useful for benchmarking across invocations, where a previous invocation has already created all objects but didn't delete them.
  #[arg(long)]
  skip_creation: bool,

  /// Skips deleting objects.
  #[arg(long)]
  skip_deletion: bool,

  /// Concurrency level. Defaults to 64.
  #[arg(long, default_value_t = default_concurrency())]
  concurrency: usize,
}

#[derive(Serialize)]
struct OpResult {
  started: DateTime<Utc>,
  exec_secs: f64,
}

#[derive(Default, Serialize)]
struct OpResults {
  #[serde(skip_serializing_if = "Option::is_none")]
  create: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  write: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  commit: Option<OpResult>,
  inspect: Option<OpResult>,
  read: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  delete: Option<OpResult>,
}

#[derive(Serialize)]
struct BenchmarkResults {
  cfg: Config,
  op: OpResults,
  final_metrics: HashMap<String, u64>,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();
  let concurrency = cli.concurrency;

  let cfg: Config =
    serde_yaml::from_str(&fs::read_to_string(&cli.config).expect("read config file"))
      .expect("parse config file");

  let mut results = BenchmarkResults {
    cfg: cfg.clone(),
    op: OpResults::default(),
    final_metrics: HashMap::new(),
  };

  let object_count = cfg.objects;
  let bucket_count = cfg.buckets.unwrap();
  let lpage_size = cfg.lpage_size.as_u64();
  let object_size = cfg.object_size.as_u64();
  let read_size = cfg.read_size.as_u64();
  let read_stream_buffer_size = cfg.read_stream_buffer_size.as_u64();
  let spage_size = cfg.spage_size.as_u64();

  let init_cfg = InitCfg {
    bucket_count,
    do_not_format_device: cli.skip_device_format,
    log_buffer_size: cfg.log_buffer_size.as_u64(),
    lpage_size,
    object_count,
    spage_size,
    partitions: cfg
      .partitions
      .into_iter()
      .map(|p| InitCfgPartition {
        len: p.len,
        offset: p.offset,
        path: p.path,
      })
      .collect(),
  };

  let store: Arc<dyn Store> = match cfg.target {
    TargetType::Direct => Arc::new(BlobdDirectStore::start(init_cfg).await),
    TargetType::KV => Arc::new(BlobdKVStore::start(init_cfg).await),
    TargetType::Lite => Arc::new(BlobdLiteStore::start(init_cfg).await),
    TargetType::FS => Arc::new(FileSystemStore::new(
      cfg.prefix.unwrap(),
      cfg.tiering.unwrap(),
    )),
    TargetType::S3 => Arc::new(cfg.s3.unwrap().build_store().await),
  };

  if !cli.skip_creation {
    let incomplete_tokens = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let create_started = Utc::now();
    let now = Instant::now();
    iter(0..object_count)
      .for_each_concurrent(concurrency, async |i| {
        let store = store.clone();
        let incomplete_tokens = incomplete_tokens.clone();
        spawn(async move {
          let res = store
            .create_object(CreateObjectInput {
              key: create_u64_be(i).into(),
              size: object_size,
            })
            .await;
          incomplete_tokens.lock().push((i, res.token));
        })
        .await
        .unwrap();
      })
      .await;
    let create_exec_secs = now.elapsed().as_secs_f64();
    results.op.create = Some(OpResult {
      started: create_started,
      exec_secs: create_exec_secs,
    });
    info!(
      create_exec_secs,
      create_ops_per_second = (object_count as f64) / create_exec_secs,
      "completed all create ops",
    );

    let write_started = Utc::now();
    let now = Instant::now();
    iter(incomplete_tokens.lock().to_vec())
      .for_each_concurrent(concurrency, async |(key, incomplete_token)| {
        let store = store.clone();
        spawn(async move {
          for offset in (0..object_size).step_by(usz!(lpage_size)) {
            let data_len = min(object_size - offset, lpage_size);
            store
              .write_object(WriteObjectInput {
                key: create_u64_be(key).into(),
                offset,
                incomplete_token: incomplete_token.clone(),
                data: &EMPTY_POOL[..usz!(data_len)],
              })
              .await;
          }
        })
        .await
        .unwrap();
      })
      .await;
    let write_exec_secs = now.elapsed().as_secs_f64();
    results.op.write = Some(OpResult {
      started: write_started,
      exec_secs: write_exec_secs,
    });
    info!(
      write_exec_secs,
      write_ops_per_second = object_count as f64 / write_exec_secs,
      write_mib_per_second =
        (object_count * object_size) as f64 / write_exec_secs / 1024.0 / 1024.0,
      "completed all write ops",
    );

    let commit_started = Utc::now();
    let now = Instant::now();
    iter(incomplete_tokens.lock().to_vec())
      .for_each_concurrent(concurrency, async |(i, incomplete_token)| {
        let store = store.clone();
        spawn(async move {
          let key = create_u64_be(i).into();
          store
            .commit_object(CommitObjectInput {
              incomplete_token: incomplete_token.clone(),
              key,
            })
            .await;
        })
        .await
        .unwrap();
      })
      .await;
    let commit_exec_secs = now.elapsed().as_secs_f64();
    results.op.commit = Some(OpResult {
      started: commit_started,
      exec_secs: commit_exec_secs,
    });
    info!(
      commit_exec_secs,
      commit_ops_per_second = (object_count as f64) / commit_exec_secs,
      "completed all commit ops",
    );
  };

  let inspect_started = Utc::now();
  let now = Instant::now();
  iter(0..object_count)
    .for_each_concurrent(concurrency, async |i| {
      let store = store.clone();
      spawn(async move {
        store
          .inspect_object(InspectObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
      })
      .await
      .unwrap();
    })
    .await;
  let inspect_exec_secs = now.elapsed().as_secs_f64();
  results.op.inspect = Some(OpResult {
    started: inspect_started,
    exec_secs: inspect_exec_secs,
  });
  info!(
    inspect_exec_secs,
    inspect_ops_per_second = (object_count as f64) / inspect_exec_secs,
    "completed all inspect ops",
  );

  let read_started = Utc::now();
  let now = Instant::now();
  iter(0..object_count)
    .for_each_concurrent(concurrency, async |i| {
      let store = store.clone();
      spawn(async move {
        for start in (0..object_size).step_by(usz!(read_size)) {
          let res = store
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
      })
      .await
      .unwrap();
    })
    .await;
  let read_exec_secs = now.elapsed().as_secs_f64();
  results.op.read = Some(OpResult {
    started: read_started,
    exec_secs: read_exec_secs,
  });
  info!(
    read_exec_secs,
    read_ops_per_second = object_count as f64 / read_exec_secs,
    read_mib_per_second = (object_count * object_size) as f64 / read_exec_secs / 1024.0 / 1024.0,
    "completed all read ops",
  );

  if !cli.skip_deletion {
    let delete_started = Utc::now();
    let now = Instant::now();
    iter(0..object_count)
      .for_each_concurrent(concurrency, async |i| {
        let store = store.clone();
        spawn(async move {
          store
            .delete_object(DeleteObjectInput {
              key: create_u64_be(i).into(),
              id: None,
            })
            .await;
        })
        .await
        .unwrap();
      })
      .await;
    let delete_exec_secs = now.elapsed().as_secs_f64();
    results.op.delete = Some(OpResult {
      started: delete_started,
      exec_secs: delete_exec_secs,
    });
    info!(
      delete_exec_secs,
      delete_ops_per_second = (object_count as f64) / delete_exec_secs,
      "completed all delete ops",
    );
  };

  store.wait_for_end().await;
  info!("blobd ended");

  let final_metrics = store.metrics();
  for (key, value) in &final_metrics {
    info!(key, value, "final metric");
  }
  results.final_metrics = final_metrics
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

  let json_output = serde_json::to_string_pretty(&results).unwrap();
  fs::write("benchmarker-results.json", json_output).unwrap();

  info!("all done");
}
