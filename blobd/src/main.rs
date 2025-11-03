use crate::endpoint::HttpCtx;
use crate::libblobd::BlobdCfg;
use crate::libblobd::BlobdLoader;
use crate::server::start_http_server_loop;
use blobd_token::BlobdTokens;
use clap::Parser;
use conf::Conf;
use data_encoding::BASE64;
#[cfg(feature = "blobd-direct")]
pub(crate) use libblobd_direct as libblobd;
#[cfg(feature = "blobd-lite")]
pub(crate) use libblobd_lite as libblobd;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::read_to_string;
use tracing::info;

pub mod conf;
pub mod endpoint;
pub mod server;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the configuration file.
  #[arg(long)]
  config: PathBuf,

  /// Format the device or file. WARNING: All existing data will be erased.
  #[arg(long)]
  format: bool,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  let conf_raw = read_to_string(cli.config).await.expect("read config file");
  let conf: Conf = toml::from_str(&conf_raw).expect("parse config file");

  #[cfg(feature = "blobd-lite")]
  let dev = {
    let io_metrics = Arc::new(seekable_async_file::SeekableAsyncFileMetrics::default());
    assert_eq!(conf.partitions.len(), 1);
    assert_eq!(conf.partitions[0].offset, 0);
    seekable_async_file::SeekableAsyncFile::open(
      &conf.partitions[0].path,
      io_metrics,
      std::time::Duration::from_micros(200),
      0,
    )
    .await
  };

  #[cfg(feature = "blobd-lite")]
  assert!(
    conf.bucket_count.is_power_of_two(),
    "bucket count must be a power of 2"
  );
  #[cfg(feature = "blobd-lite")]
  assert!(
    conf.bucket_count >= 4096 && conf.bucket_count <= 281474976710656,
    "bucket count must be in the range [4096, 281474976710656]"
  );

  let token_secret: [u8; 32] = BASE64
    .decode(conf.token_secret_base64.as_bytes())
    .expect("decode configured token secret")
    .try_into()
    .expect("configured token secret must have length of 32");
  let tokens = BlobdTokens::new(token_secret.clone());

  #[cfg(feature = "blobd-lite")]
  let blobd = BlobdLoader::new(dev.clone(), conf.partitions[0].len, BlobdCfg {
    bucket_count_log2: conf.bucket_count.ilog2().try_into().unwrap(),
    bucket_lock_count_log2: conf.bucket_lock_count_log2,
    lpage_size_pow2: conf.lpage_size_pow2,
    spage_size_pow2: conf.spage_size_pow2,
    reap_objects_after_secs: conf.reap_objects_after_secs,
    versioning: conf.versioning,
  });
  #[cfg(feature = "blobd-direct")]
  let blobd = BlobdLoader::new(
    conf
      .partitions
      .into_iter()
      .map(|p| libblobd::BlobdCfgPartition {
        len: p.len,
        offset: p.offset,
        path: p.path,
      })
      .collect(),
    BlobdCfg {
      backing_store: libblobd::BlobdCfgBackingStore::Uring,
      expire_incomplete_objects_after_secs: conf.reap_objects_after_secs,
      lpage_size_pow2: conf.lpage_size_pow2,
      object_tuples_area_reserved_space: conf.object_tuples_area_reserved_space,
      spage_size_pow2: conf.spage_size_pow2,
      uring_coop_taskrun: false,
      uring_defer_taskrun: false,
      uring_iopoll: false,
      uring_sqpoll: None,
    },
  );

  if cli.format {
    blobd.format().await;
    info!("device formatted");
    return;
  };

  #[cfg(feature = "blobd-lite")]
  let blobd = {
    let blobd = blobd.load().await;
    tokio::spawn({
      let dev = dev.clone();
      async move {
        dev.start_delayed_data_sync_background_loop().await;
      }
    });
    tokio::spawn({
      let blobd = blobd.clone();
      async move {
        blobd.clone().start().await;
      }
    });
    blobd
  };
  #[cfg(feature = "blobd-direct")]
  let blobd = blobd.load_and_start().await;

  let ctx = Arc::new(HttpCtx {
    authentication_is_enabled: !conf.disable_authentication,
    blobd: blobd.clone(),
    token_secret: token_secret.clone(),
    tokens,
  });

  start_http_server_loop(conf.interface, conf.port, ctx).await;
}
