use crate::endpoint::HttpCtx;
use crate::server::start_http_server_loop;
use aes_gcm::Aes256Gcm;
use aes_gcm::KeyInit;
use blobd_token::BlobdTokens;
use clap::Parser;
use conf::Conf;
use data_encoding::BASE64;
use libblobd::BlobdLoader;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::read_to_string;
use tokio::join;
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

  let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());

  let dev_size = get_file_len_via_seek(&conf.device_path)
    .await
    .expect("seek device file");
  let dev = SeekableAsyncFile::open(
    &conf.device_path,
    dev_size,
    io_metrics,
    Duration::from_micros(200),
    0,
  )
  .await;

  assert!(
    conf.bucket_count.is_power_of_two(),
    "bucket count must be a power of 2"
  );
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

  let blobd = BlobdLoader::new(
    dev.clone(),
    dev_size,
    conf.bucket_count.ilog2().try_into().unwrap(),
  );

  if cli.format {
    blobd.format().await;
    info!("device formatted");
    return;
  };

  let blobd = blobd.load().await;

  let ctx = Arc::new(HttpCtx {
    authentication_is_enabled: !conf.disable_authentication,
    blobd: blobd.clone(),
    token_secret_aes_gcm: Aes256Gcm::new(&token_secret.into()),
    tokens,
  });

  join! {
    blobd.start(),
    dev.start_delayed_data_sync_background_loop(),
    start_http_server_loop(conf.interface, conf.port, ctx),
  };
}
