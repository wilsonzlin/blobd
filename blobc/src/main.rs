pub mod upload;

use blobd_client_rs::BlobdClient;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use std::path::PathBuf;

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

  /// Maximum concurrent uploads. Defaults to 8.
  #[arg(default_value_t = 8)]
  concurrency: usize,
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
  // TODO
}
