pub mod upload;

use blobd_client_rs::BlobdClient;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use data_encoding::BASE64;
use dirs::config_dir;
use serde::Deserialize;
use serde::Serialize;
use std::env::var_os;
use std::fs::read_to_string;
use std::path::PathBuf;
use upload::cmd_upload;

#[derive(Serialize, Deserialize)]
struct Conf {
  endpoint: String,
  token_secret: String,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
  /// Override blobd server endpoint e.g. https://my.blobd.turbod.io, http://127.0.0.1:9981.
  #[arg(short, long)]
  endpoint: Option<String>,

  #[command(subcommand)]
  command: Commands,
}

#[derive(Args, Debug)]
struct CmdUpload {
  /// Prefix all local files with this key.
  prefix: Option<String>,

  /// Path to local directory to use as source of truth. New and changed files will overwrite blobd objects.
  source: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Commands {
  Upload(CmdUpload),
}

struct Ctx {
  client: BlobdClient,
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
    client: BlobdClient::new(endpoint, key),
  };
  match cli.command {
    Commands::Upload(cmd) => cmd_upload(ctx, cmd),
  }
  .await;
}
