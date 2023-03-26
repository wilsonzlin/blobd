use crate::bucket::Buckets;
use crate::bucket::BUCKETS_SIZE;
use crate::ctx::Ctx;
use crate::ctx::FreeListWithChangeTracker;
use crate::ctx::SequentialisedJournal;
use crate::free_list::FreeList;
use crate::free_list::FREELIST_SIZE;
use crate::object_id::ObjectIdSerial;
use crate::server::start_http_server_loop;
use crate::stream::Stream;
use crate::stream::STREAM_SIZE;
use crate::tile::TILE_SIZE;
use clap::Parser;
use conf::Conf;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::read_to_string;
use tokio::join;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use write_journal::WriteJournal;

pub mod bucket;
pub mod conf;
pub mod ctx;
pub mod endpoint;
pub mod free_list;
pub mod inode;
pub mod object_id;
pub mod server;
pub mod stream;
pub mod tile;

/**

DEVICE
======

Structure
---------

journal
object_id_serial
stream
free_list
buckets
heap

**/

fn div_ceil(n: u64, d: u64) -> u64 {
  (n / d) + ((n % d != 0) as u64)
}

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
    std::time::Duration::from_micros(200),
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

  let offsetof_journal = 0;
  let sizeof_journal = 1024 * 1024 * 8;
  let offsetof_object_id_serial = offsetof_journal + sizeof_journal;
  let offsetof_stream = offsetof_object_id_serial + 8;
  let offsetof_free_list = offsetof_stream + STREAM_SIZE;
  let offsetof_buckets = offsetof_free_list + FREELIST_SIZE();
  let sizeof_buckets = BUCKETS_SIZE(conf.bucket_count);
  let reserved_space = offsetof_buckets + sizeof_buckets;
  let reserved_tiles = div_ceil(reserved_space, u64::from(TILE_SIZE));

  let journal = WriteJournal::new(dev.clone(), offsetof_journal, sizeof_journal);

  if cli.format {
    journal.format_device().await;
    ObjectIdSerial::format_device(&dev, offsetof_object_id_serial);
    Stream::format_device(&dev, offsetof_stream);
    FreeList::format_device(&dev, offsetof_free_list);
    Buckets::format_device(&dev, offsetof_buckets, conf.bucket_count);
    dev.sync_data().await;
    return;
  };

  let object_id_serial = ObjectIdSerial::load_from_device(&dev, offsetof_object_id_serial);
  let stream = Stream::load_from_device(&dev, offsetof_stream);
  let free_list = FreeList::load_from_device(
    &dev,
    offsetof_free_list,
    reserved_tiles.try_into().unwrap(),
    (dev_size / u64::from(TILE_SIZE)).try_into().unwrap(),
  );
  let buckets = Buckets::load_from_device(dev.clone(), offsetof_buckets);

  let ctx = Arc::new(Ctx {
    buckets,
    device: dev.clone(),
    free_list: Mutex::new(FreeListWithChangeTracker::new(free_list)),
    journal: Mutex::new(SequentialisedJournal::new(journal)),
    object_id_serial,
    stream: RwLock::new(stream),
  });

  join! {
    start_http_server_loop(conf.interface, conf.port, ctx),
    dev.start_delayed_data_sync_background_loop(),
  };
}
