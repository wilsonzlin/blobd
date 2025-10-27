use ahash::HashMap;
use ahash::HashMapExt;
use blobd_benchmarker::BenchmarkResults;
use blobd_benchmarker::Config;
use blobd_benchmarker::OpMetricsSample;
use blobd_benchmarker::OpResult;
use blobd_benchmarker::OpResults;
use blobd_benchmarker::SystemMetricsSample;
use blobd_benchmarker::TargetType;
use chrono::Utc;
use clap::Parser;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::create_u64_be;
use off64::usz;
use procfs::Current;
use procfs::CurrentSI;
use rand::thread_rng;
use rand::RngCore;
use std::cmp::min;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use store::direct::BlobdDirectStore;
use store::fs::FileSystemStore;
use store::kv::BlobdKVStore;
use store::lite::BlobdLiteStore;
use store::rocksdb::RocksDBStore;
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

#[derive(Parser)]
struct Cli {
  /// Benchmark folders to run (comma-separated). If not specified, runs all benchmarks in cfg/.
  benchmarks: Option<String>,

  /// Number of buckets to allocate (overrides config)
  #[arg(long)]
  buckets: Option<u64>,

  /// Number of objects to create (overrides config)
  #[arg(long)]
  objects: Option<u64>,

  /// Size of each object in bytes (overrides config)
  #[arg(long)]
  object_size: Option<u64>,

  /// Concurrency level (overrides config)
  #[arg(long)]
  concurrency: Option<usize>,

  /// Skips formatting the device.
  #[arg(long)]
  skip_device_format: bool,

  /// Skips creating, writing, and committing objects. Useful for benchmarking across invocations, where a previous invocation has already created all objects but didn't delete them.
  #[arg(long)]
  skip_creation: bool,

  /// Skips deleting objects.
  #[arg(long)]
  skip_deletion: bool,
}

fn run_script(script_path: &Path) {
  info!(script = %script_path.display(), "running script");
  let status = Command::new("bash")
    .arg(script_path)
    .stdout(Stdio::inherit())
    .stderr(Stdio::inherit())
    .status()
    .expect("failed to execute script");

  assert!(
    status.success(),
    "script {} failed with status: {}",
    script_path.display(),
    status
  );
}

fn malloc_trim() {
  info!("trimming malloc");
  unsafe {
    libc::malloc_trim(0);
  }
}

fn drop_caches() {
  info!("dropping kernel caches");
  // First sync to flush buffers
  unsafe {
    libc::sync();
  }
  // Drop caches (requires root)
  fs::write("/proc/sys/vm/drop_caches", "3").expect("failed to drop caches (requires root)");
}

fn cleanup_memory() {
  malloc_trim();
  drop_caches();
}

struct MetricsCollector {
  samples: Arc<parking_lot::Mutex<Vec<SystemMetricsSample>>>,
  stop_signal: Arc<AtomicBool>,
  handle: std::thread::JoinHandle<()>,
}

impl MetricsCollector {
  fn new() -> Self {
    let samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let stop_signal = Arc::new(AtomicBool::new(false));

    let handle = std::thread::spawn({
      let samples = samples.clone();
      let stop_signal = stop_signal.clone();
      move || {
        let interval = std::time::Duration::from_secs(1);

        #[derive(Default, Clone)]
        struct DiskCounters {
          read_bytes: u64,
          write_bytes: u64,
          read_ops: u64,
          write_ops: u64,
          read_merges: u64,
          write_merges: u64,
          io_ticks: u64,
          time_in_queue: u64,
        }

        // Read /proc/stat using procfs - cumulative CPU ticks since boot (all cores combined)
        fn read_cpu_ticks() -> (u64, u64) {
          let stat = procfs::KernelStats::current().unwrap();
          let cpu_total = &stat.total;
          let user = cpu_total.user + cpu_total.nice;
          let system = cpu_total.system;
          (user, system)
        }

        // Read /proc/meminfo using procfs
        fn read_memory() -> (u64, u64) {
          let meminfo = procfs::Meminfo::current().unwrap();
          let total = meminfo.mem_total;
          let available = meminfo.mem_available.unwrap_or(meminfo.mem_free);
          let used = total.saturating_sub(available);
          (used, total)
        }

        // Read /proc/diskstats using procfs - cumulative disk I/O since boot (all disks combined)
        fn read_disk_stats() -> (DiskCounters, u64) {
          let mut counters = DiskCounters::default();
          let mut in_flight = 0u64;

          if let Ok(diskstats) = procfs::diskstats() {
            for stat in &diskstats {
              counters.read_bytes += stat.sectors_read * 512;
              counters.write_bytes += stat.sectors_written * 512;
              counters.read_ops += stat.reads;
              counters.write_ops += stat.writes;
              counters.read_merges += stat.merged;
              counters.write_merges += stat.writes_merged;
              in_flight += stat.in_progress as u64;
              counters.io_ticks += stat.time_in_progress;
              counters.time_in_queue += stat.weighted_time_in_progress;
            }
          }

          (counters, in_flight)
        }

        // Get baseline readings on first sample - will subtract these to get "since benchmark start"
        let mut baseline_cpu: Option<(u64, u64)> = None;
        let clock_ticks_per_sec = procfs::ticks_per_second() as f64;
        let mut baseline_disk: Option<DiskCounters> = None;

        while !stop_signal.load(Ordering::Relaxed) {
          let loop_start = std::time::Instant::now();

          // Read current CPU ticks
          let (curr_user, curr_system) = read_cpu_ticks();
          let baseline = baseline_cpu.get_or_insert((curr_user, curr_system));
          let cpu_user_ticks = curr_user.saturating_sub(baseline.0);
          let cpu_system_ticks = curr_system.saturating_sub(baseline.1);
          let cpu_user_secs = cpu_user_ticks as f64 / clock_ticks_per_sec;
          let cpu_system_secs = cpu_system_ticks as f64 / clock_ticks_per_sec;

          // Read current memory usage
          let (memory_used_bytes, memory_total_bytes) = read_memory();

          // Read current disk stats
          let (curr, disk_in_flight) = read_disk_stats();
          let baseline = baseline_disk.get_or_insert(curr.clone());

          // Store cumulative values since benchmark start (subtract baseline)
          let sample = SystemMetricsSample {
            timestamp: Utc::now(),
            cpu_user_secs,
            cpu_system_secs,
            memory_used_bytes,
            memory_total_bytes,
            disk_read_bytes: curr.read_bytes.saturating_sub(baseline.read_bytes),
            disk_write_bytes: curr.write_bytes.saturating_sub(baseline.write_bytes),
            disk_read_ops: curr.read_ops.saturating_sub(baseline.read_ops),
            disk_write_ops: curr.write_ops.saturating_sub(baseline.write_ops),
            disk_read_merges: curr.read_merges.saturating_sub(baseline.read_merges),
            disk_write_merges: curr.write_merges.saturating_sub(baseline.write_merges),
            disk_in_flight,
            disk_io_ticks: curr.io_ticks.saturating_sub(baseline.io_ticks),
            disk_time_in_queue: curr.time_in_queue.saturating_sub(baseline.time_in_queue),
          };

          samples.lock().push(sample);

          // Sleep for the remaining interval time
          let elapsed = loop_start.elapsed();
          if elapsed < interval {
            std::thread::sleep(interval - elapsed);
          }
        }
      }
    });

    Self {
      samples,
      stop_signal,
      handle,
    }
  }

  fn stop(self) -> Vec<SystemMetricsSample> {
    self.stop_signal.store(true, Ordering::Relaxed);
    self.handle.join().unwrap();
    Arc::try_unwrap(self.samples).ok().unwrap().into_inner()
  }
}

struct OpMetricsTracker {
  ops_completed: Arc<AtomicU64>,
  bytes_transferred: Arc<AtomicU64>,
  samples: Arc<parking_lot::Mutex<Vec<OpMetricsSample>>>,
  stop_signal: Arc<AtomicBool>,
  handle: Option<std::thread::JoinHandle<()>>,
}

impl Clone for OpMetricsTracker {
  fn clone(&self) -> Self {
    Self {
      ops_completed: self.ops_completed.clone(),
      bytes_transferred: self.bytes_transferred.clone(),
      samples: self.samples.clone(),
      stop_signal: self.stop_signal.clone(),
      handle: None,
    }
  }
}

impl OpMetricsTracker {
  fn new() -> Self {
    let ops_completed = Arc::new(AtomicU64::new(0));
    let bytes_transferred = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let stop_signal = Arc::new(AtomicBool::new(false));

    let handle = std::thread::spawn({
      let ops_completed = ops_completed.clone();
      let bytes_transferred = bytes_transferred.clone();
      let samples = samples.clone();
      let stop_signal = stop_signal.clone();
      move || {
        let mut prev_ops = 0u64;
        let mut prev_bytes = 0u64;
        while !stop_signal.load(Ordering::Relaxed) {
          std::thread::sleep(std::time::Duration::from_secs(1));
          let curr_ops = ops_completed.load(Ordering::Relaxed);
          let curr_bytes = bytes_transferred.load(Ordering::Relaxed);
          samples.lock().push(OpMetricsSample {
            timestamp: Utc::now(),
            ops_completed: curr_ops - prev_ops,
            bytes_transferred: curr_bytes - prev_bytes,
          });
          prev_ops = curr_ops;
          prev_bytes = curr_bytes;
        }
      }
    });

    Self {
      ops_completed,
      bytes_transferred,
      samples,
      stop_signal,
      handle: Some(handle),
    }
  }

  fn inc_ops(&self) {
    self.ops_completed.fetch_add(1, Ordering::Relaxed);
  }

  fn add_bytes(&self, bytes: u64) {
    self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
  }

  fn finish(mut self) -> Vec<OpMetricsSample> {
    self.stop_signal.store(true, Ordering::Relaxed);
    if let Some(handle) = self.handle.take() {
      handle.join().ok();
    }
    self.samples.lock().clone()
  }
}

async fn run_benchmark(benchmark_name: String, benchmark_dir: PathBuf, cli: &Cli) {
  info!(benchmark = %benchmark_name, "running benchmark");

  let cfg_file = benchmark_dir.join("cfg.yaml");
  let start_script = benchmark_dir.join("start.sh");
  let stop_script = benchmark_dir.join("stop.sh");

  // Parse config first to resolve values
  let cfg: Config = serde_yaml::from_str(&fs::read_to_string(&cfg_file).expect("read config file"))
    .expect("parse config file");

  // Resolve effective values (CLI overrides config)
  let buckets = cli.buckets.or(cfg.buckets).unwrap_or(0);
  let objects = cli
    .objects
    .or(cfg.objects)
    .expect("objects must be specified in config or CLI");
  let object_size = cli
    .object_size
    .or(cfg.object_size)
    .expect("object_size must be specified in config or CLI");
  let concurrency = cli
    .concurrency
    .or(cfg.concurrency)
    .expect("concurrency must be specified in config or CLI");

  // Construct results file path using resolved values
  let results_file = benchmark_dir.join(format!("results.{}o.{}b.json", objects, object_size));

  // Run start.sh if it exists
  let ran_start_script = if start_script.exists() {
    run_script(&start_script);
    true
  } else {
    info!("no start.sh found, skipping");
    false
  };

  // Start metrics collection (sample every 1 second)
  let metrics_collector = MetricsCollector::new();

  // Initialize random pool based on object size
  info!(object_size, "initializing random pool");
  let mut rand_pool = vec![0u8; usz!(object_size)];
  thread_rng().fill_bytes(&mut rand_pool);
  let rand_pool = Arc::new(rand_pool);

  let mut results = BenchmarkResults {
    benchmark_name: benchmark_name.clone(),
    cfg: cfg.clone(),
    buckets,
    objects,
    object_size,
    concurrency,
    op: OpResults::default(),
    wait_for_end_secs: 0.0,
    store_metrics: HashMap::<String, u64>::new(),
    system_metrics: Vec::new(),
  };

  let object_count = objects;
  let bucket_count = buckets;
  let lpage_size = cfg.lpage_size.as_u64();
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
      cfg.prefix.clone().unwrap(),
      cfg.tiering.unwrap(),
    )),
    TargetType::S3 => Arc::new(cfg.s3.unwrap().build_store().await),
    TargetType::RocksDB => Arc::new(RocksDBStore::new(
      cfg.prefix.unwrap().to_str().unwrap(),
      cfg.use_block_cache,
    )),
  };

  if !cli.skip_creation {
    let incomplete_tokens = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let create_started = Utc::now();
    let now = Instant::now();
    let tracker = OpMetricsTracker::new();

    iter(0..object_count)
      .for_each_concurrent(concurrency, async |i| {
        let store = store.clone();
        let incomplete_tokens = incomplete_tokens.clone();
        let tracker = tracker.clone();
        spawn(async move {
          let res = store
            .create_object(CreateObjectInput {
              key: create_u64_be(i).into(),
              size: object_size,
            })
            .await;
          incomplete_tokens.lock().push((i, res.token));
          tracker.inc_ops();
        })
        .await
        .unwrap();
      })
      .await;

    let create_exec_secs = now.elapsed().as_secs_f64();
    results.op.create = Some(OpResult {
      started: create_started,
      exec_secs: create_exec_secs,
      samples: tracker.finish(),
    });
    info!(
      create_exec_secs,
      create_ops_per_second = (object_count as f64) / create_exec_secs,
      "completed all create ops",
    );

    let write_started = Utc::now();
    let now = Instant::now();
    let tracker = OpMetricsTracker::new();

    iter(incomplete_tokens.lock().to_vec())
      .for_each_concurrent(concurrency, async |(key, incomplete_token)| {
        let store = store.clone();
        let tracker = tracker.clone();
        let rand_pool = rand_pool.clone();
        spawn(async move {
          let write_chunk_size = store.write_chunk_size();
          for offset in (0..object_size).step_by(usz!(write_chunk_size)) {
            let data_len = min(object_size - offset, write_chunk_size);
            store
              .write_object(WriteObjectInput {
                key: create_u64_be(key).into(),
                offset,
                incomplete_token: incomplete_token.clone(),
                data: &rand_pool[usz!(offset)..usz!(offset + data_len)],
              })
              .await;
            tracker.add_bytes(data_len);
          }
          tracker.inc_ops();
        })
        .await
        .unwrap();
      })
      .await;

    let write_exec_secs = now.elapsed().as_secs_f64();
    results.op.write = Some(OpResult {
      started: write_started,
      exec_secs: write_exec_secs,
      samples: tracker.finish(),
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
    let tracker = OpMetricsTracker::new();

    iter(incomplete_tokens.lock().to_vec())
      .for_each_concurrent(concurrency, async |(i, incomplete_token)| {
        let store = store.clone();
        let tracker = tracker.clone();
        spawn(async move {
          let key = create_u64_be(i).into();
          store
            .commit_object(CommitObjectInput {
              incomplete_token: incomplete_token.clone(),
              key,
            })
            .await;
          tracker.inc_ops();
        })
        .await
        .unwrap();
      })
      .await;

    let commit_exec_secs = now.elapsed().as_secs_f64();
    results.op.commit = Some(OpResult {
      started: commit_started,
      exec_secs: commit_exec_secs,
      samples: tracker.finish(),
    });
    info!(
      commit_exec_secs,
      commit_ops_per_second = (object_count as f64) / commit_exec_secs,
      "completed all commit ops",
    );
  };

  let inspect_started = Utc::now();
  let now = Instant::now();
  let tracker = OpMetricsTracker::new();

  iter(0..object_count)
    .for_each_concurrent(concurrency, async |i| {
      let store = store.clone();
      let tracker = tracker.clone();
      spawn(async move {
        store
          .inspect_object(InspectObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
        tracker.inc_ops();
      })
      .await
      .unwrap();
    })
    .await;

  let inspect_exec_secs = now.elapsed().as_secs_f64();
  results.op.inspect = Some(OpResult {
    started: inspect_started,
    exec_secs: inspect_exec_secs,
    samples: tracker.finish(),
  });
  info!(
    inspect_exec_secs,
    inspect_ops_per_second = (object_count as f64) / inspect_exec_secs,
    "completed all inspect ops",
  );

  let read_started = Utc::now();
  let now = Instant::now();
  let tracker = OpMetricsTracker::new();

  // Evict kernel block cache before reads to ensure we measure actual disk I/O
  drop_caches();

  iter(0..object_count)
    .for_each_concurrent(concurrency, async |i| {
      let store = store.clone();
      let tracker = tracker.clone();
      let rand_pool = rand_pool.clone();
      spawn(async move {
        for start in (0..object_size).step_by(usz!(read_size)) {
          let read_len = min(object_size, start + read_size) - start;
          let mut res = store
            .read_object(ReadObjectInput {
              key: create_u64_be(i).into(),
              id: None,
              start,
              end: Some(start + read_len),
              stream_buffer_size: read_stream_buffer_size,
            })
            .await;
          // Do something sophisticated with the response data so the compiler doesnt just drop it and we get insane "read throughput".
          // WARNING: At max opt levels, the compiler is very aggressive! Even something like checking if len() is greater than 0 may mean compiler drops as soon as one byte is read! Or if only checking length, then it may just skip reading data and just check length only.
          let mut offset = usz!(start);
          while let Some(chunk) = res.data_stream.next().await {
            assert_eq!(chunk, rand_pool[offset..offset + chunk.len()]);
            offset += chunk.len();
          }
          tracker.add_bytes(read_len);
        }
        tracker.inc_ops();
      })
      .await
      .unwrap();
    })
    .await;

  let read_exec_secs = now.elapsed().as_secs_f64();
  results.op.read = Some(OpResult {
    started: read_started,
    exec_secs: read_exec_secs,
    samples: tracker.finish(),
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
    let tracker = OpMetricsTracker::new();

    iter(0..object_count)
      .for_each_concurrent(concurrency, async |i| {
        let store = store.clone();
        let tracker = tracker.clone();
        spawn(async move {
          store
            .delete_object(DeleteObjectInput {
              key: create_u64_be(i).into(),
              id: None,
            })
            .await;
          tracker.inc_ops();
        })
        .await
        .unwrap();
      })
      .await;

    let delete_exec_secs = now.elapsed().as_secs_f64();
    results.op.delete = Some(OpResult {
      started: delete_started,
      exec_secs: delete_exec_secs,
      samples: tracker.finish(),
    });
    info!(
      delete_exec_secs,
      delete_ops_per_second = (object_count as f64) / delete_exec_secs,
      "completed all delete ops",
    );
  };

  info!("waiting for store to end (flush/compact/etc)");
  let wait_start = Instant::now();
  store.wait_for_end().await;
  let wait_for_end_secs = wait_start.elapsed().as_secs_f64();
  info!(wait_for_end_secs, "store ended");
  results.wait_for_end_secs = wait_for_end_secs;

  let store_metrics = store.metrics();
  for (key, value) in &store_metrics {
    info!(key, value, "store metric");
  }
  results.store_metrics = store_metrics
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

  // Drop store to release all file handles before cleanup
  info!("dropping store to release file handles");
  drop(store);

  // Stop metrics collection
  info!("stopping metrics collection");
  results.system_metrics = metrics_collector.stop();

  // Run stop.sh if start.sh was run
  if ran_start_script && stop_script.exists() {
    run_script(&stop_script);
  }

  // Always cleanup memory after benchmarking
  cleanup_memory();

  // Write results to benchmark folder
  let json_output = serde_json::to_string_pretty(&results).expect("failed to serialize results");
  fs::write(&results_file, json_output).expect("failed to write results file");
  info!(results_file = %results_file.display(), "results written");

  info!(benchmark = %benchmark_name, "benchmark complete");
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  // Determine which benchmarks to run
  let cfg_dir = PathBuf::from("cfg");
  let benchmark_names: Vec<String> = if let Some(ref benchmarks) = cli.benchmarks {
    benchmarks
      .split(',')
      .map(|s| s.trim().to_string())
      .collect()
  } else {
    // Run all benchmarks in cfg/ directory
    fs::read_dir(&cfg_dir)
      .expect("failed to read cfg directory")
      .filter_map(|entry| {
        let entry = entry.ok()?;
        if entry.file_type().ok()?.is_dir() {
          entry.file_name().to_str().map(|s| s.to_string())
        } else {
          None
        }
      })
      .collect()
  };

  info!(benchmarks = ?benchmark_names, "running benchmarks");

  for benchmark_name in benchmark_names {
    let benchmark_dir = cfg_dir.join(&benchmark_name);
    assert!(
      benchmark_dir.exists(),
      "benchmark directory not found: {}",
      benchmark_dir.display()
    );
    run_benchmark(benchmark_name.clone(), benchmark_dir, &cli).await;
  }

  info!("all benchmarks complete");
}
