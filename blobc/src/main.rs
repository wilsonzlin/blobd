pub mod get;
pub mod put;
pub mod upload_dir;

use blobd_client_rs::BlobdClient;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use data_encoding::BASE64;
use dirs::config_dir;
use get::cmd_get;
use put::cmd_put;
use serde::Deserialize;
use serde::Serialize;
use std::env::var_os;
use std::fs::read_to_string;
use std::path::PathBuf;
use std::sync::Arc;
use upload_dir::cmd_upload_dir;

#[derive(Serialize, Deserialize)]
struct Conf {
  endpoint: String,
  token_secret: String,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
  /// Override blobd server endpoint e.g. https://my.blobd.turbod.io, http://127.0.0.1:9981.
  #[arg(long)]
  endpoint: Option<String>,

  #[command(subcommand)]
  command: Commands,
}

#[derive(Args, Debug)]
/// Fetch an object's data.
struct CmdGet {
  /// Upload the file to a new object with this key.
  key: String,

  /// Offset in data to fetch from.
  #[arg(long, default_value_t = 0)]
  start: u64,

  /// Offset in data to fetch to.
  #[arg(long)]
  end: Option<u64>,
}

#[derive(Args, Debug)]
/// Upload a local file.
struct CmdPut {
  /// Upload the file to a new object with this key.
  #[arg(long)]
  destination: String,

  /// Local file to upload.
  #[arg(long)]
  source: PathBuf,

  /// How many parts to upload simultaneously.
  #[arg(long, default_value_t = 8)]
  concurrency: usize,
}

#[derive(Args, Debug)]
/// Upload a local directory recursively.
struct CmdUploadDir {
  /// Prefix all local files with this key.
  #[arg(long)]
  prefix: Option<String>,

  /// Path to local directory to use as source of truth. New and changed files will overwrite blobd objects.
  #[arg(long)]
  source: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Commands {
  Get(CmdGet),
  Put(CmdPut),
  UploadDir(CmdUploadDir),
}

struct Ctx {
  client: Arc<BlobdClient>,
}

#[tokio::main]
async fn main() {
  let conf_path = var_os("BLOBC_CONF")
    .map(|p| PathBuf::from(p))
    .unwrap_or_else(|| {
      config_dir()
        .expect("get local config dir")
        .join("blobc.toml")
    });
  let conf_raw = read_to_string(conf_path).expect("read local config");
  let conf: Conf = toml::from_str(&conf_raw).expect("parse local config");

  let cli = Cli::parse();
  let endpoint = cli.endpoint.unwrap_or(conf.endpoint);
  let key: [u8; 32] = BASE64
    .decode(conf.token_secret.as_bytes())
    .expect("decode base64 token secret")
    .try_into()
    .expect("token secret must be 32 bytes after decoding");

  let ctx = Ctx {
    client: Arc::new(BlobdClient::new(endpoint, key)),
  };
  match cli.command {
    Commands::Get(cmd) => cmd_get(ctx, cmd).await,
    Commands::Put(cmd) => cmd_put(ctx, cmd).await,
    Commands::UploadDir(cmd) => cmd_upload_dir(ctx, cmd).await,
  };
}
