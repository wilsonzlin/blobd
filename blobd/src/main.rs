use crate::endpoint::HttpCtx;
use crate::server::start_http_server_loop;
use blobd_token::BlobdTokens;
use clap::Parser;
use conf::Conf;
use data_encoding::BASE64;
use libblobd_direct::BlobdCfg;
use libblobd_direct::BlobdCfgBackingStore;
use libblobd_direct::BlobdCfgPartition;
use libblobd_direct::BlobdLoader;
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

  let token_secret: [u8; 32] = BASE64
    .decode(conf.token_secret_base64.as_bytes())
    .expect("decode configured token secret")
    .try_into()
    .expect("configured token secret must have length of 32");
  let tokens = BlobdTokens::new(token_secret.clone());

  let blobd = BlobdLoader::new(
    conf
      .partitions
      .into_iter()
      .map(|p| BlobdCfgPartition {
        len: p.len,
        offset: p.offset,
        path: p.path,
      })
      .collect(),
    BlobdCfg {
      backing_store: BlobdCfgBackingStore::Uring,
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

  let blobd = blobd.load_and_start().await;

  let ctx = Arc::new(HttpCtx {
    authentication_is_enabled: !conf.disable_authentication,
    blobd: blobd.clone(),
    token_secret: token_secret.clone(),
    tokens,
  });

  start_http_server_loop(conf.interface, conf.port, ctx).await;
}
