use ahash::HashMap;
use bytesize::ByteSize;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;

// This is redefined here from store::s3::S3StoreConfig to avoid a dependency on the store crate which is giant and takes a long time to compile.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3StoreConfig {
  pub region: String,
  pub endpoint: String,
  pub access_key_id: String,
  pub secret_access_key: String,
  pub bucket: String,
  pub part_size: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Deserialize, Serialize)]
pub enum TargetType {
  Direct,
  KV,
  Lite,
  FS,
  S3,
  RocksDB,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ConfigPartition {
  pub path: PathBuf,
  pub offset: u64,
  pub len: u64,
}

fn default_read_size() -> ByteSize {
  ByteSize::mib(4)
}

fn default_read_stream_buffer_size() -> ByteSize {
  ByteSize::kib(16)
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

fn default_use_block_cache() -> bool {
  true
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
  pub target: TargetType,

  /// Only applicable for the "fs" target.
  pub prefix: Option<PathBuf>,

  /// Only applicable for the "fs" target.
  pub tiering: Option<usize>,

  /// Only applicable for the "s3" target.
  pub s3: Option<S3StoreConfig>,

  /// For the "lite" target, there must only be one partition and its offset must be zero.
  #[serde(default)]
  pub partitions: Vec<ConfigPartition>,

  /// Read size. Defaults to 4 MiB.
  #[serde(default = "default_read_size")]
  pub read_size: ByteSize,

  /// Read stream buffer size. Defaults to 16 KiB.
  #[serde(default = "default_read_stream_buffer_size")]
  pub read_stream_buffer_size: ByteSize,

  /// Lpage size. Defaults to 16 MiB.
  #[serde(default = "default_lpage_size")]
  pub lpage_size: ByteSize,

  /// Spage size. Defaults to 512 bytes.
  #[serde(default = "default_spage_size")]
  pub spage_size: ByteSize,

  /// Only applies to Kv target. Defaults to 1 GiB.
  #[serde(default = "default_log_buffer_size")]
  pub log_buffer_size: ByteSize,

  /// Number of buckets to allocate. Can be overridden via CLI.
  pub buckets: Option<u64>,

  /// Number of objects to create. Can be overridden via CLI.
  pub objects: Option<u64>,

  /// Size of each object in bytes. Can be overridden via CLI.
  pub object_size: Option<u64>,

  /// Concurrency level. Can be overridden via CLI.
  pub concurrency: Option<usize>,

  #[serde(default = "default_use_block_cache")]
  pub use_block_cache: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LatencyStats {
  pub avg_ms: f64,
  pub p95_ms: f64,
  pub p99_ms: f64,
  pub max_ms: f64,
}

#[derive(Serialize, Deserialize)]
pub struct OpResult {
  pub started: DateTime<Utc>,
  pub exec_secs: f64,
  pub latency: LatencyStats,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub ttfb: Option<LatencyStats>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct OpResults {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub create: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub write: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub commit: Option<OpResult>,
  pub inspect: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub random_read: Option<OpResult>,
  pub read: Option<OpResult>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub delete: Option<OpResult>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct FinalSystemMetrics {
  pub peak_memory_bytes: u64,
  pub total_cpu_user_secs: f64,
  pub total_cpu_system_secs: f64,
  pub total_disk_read_bytes: u64,
  pub total_disk_write_bytes: u64,
  pub total_disk_read_ops: u64,
  pub total_disk_write_ops: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BenchmarkResults {
  pub benchmark_name: String,
  pub cfg: Config,
  pub buckets: u64,
  pub objects: u64,
  pub object_size: u64,
  pub concurrency: usize,
  pub op: OpResults,
  pub wait_for_end_secs: f64,
  pub store_metrics: HashMap<String, u64>,
  pub system_metrics: FinalSystemMetrics,
}

impl BenchmarkResults {
  pub fn label(&self) -> String {
    let mut parts = vec![self.benchmark_name.clone()];
    parts.push(format!("{} objects", self.objects));
    if let Some(tiering) = self.cfg.tiering {
      parts.push(format!("tiering {}", tiering));
    }
    if self.buckets > 0 {
      parts.push(format!("{} buckets", self.buckets));
    }
    format!("{} ({})", parts[0], parts[1..].join(", "))
  }

  pub fn should_skip_create(&self) -> bool {
    matches!(self.cfg.target, TargetType::RocksDB)
  }

  pub fn should_skip_commit(&self) -> bool {
    matches!(self.cfg.target, TargetType::RocksDB | TargetType::FS)
  }
}
