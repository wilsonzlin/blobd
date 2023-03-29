use crate::bucket::Buckets;
use crate::free_list::FreeList;
use crate::free_list::FREELIST_TILE_CAP;
use crate::object_id::ObjectIdSerial;
use crate::stream::Stream;
use crate::stream::STREAM_SIZE;
use crate::tile::TILE_SIZE_U64;
use bucket::BUCKETS_SIZE;
use ctx::Ctx;
use ctx::FreeListWithChangeTracker;
use ctx::SequentialisedJournal;
use free_list::FREELIST_SIZE;
use futures::join;
use op::commit_object::op_commit_object;
use op::commit_object::OpCommitObjectInput;
use op::commit_object::OpCommitObjectOutput;
use op::create_object::op_create_object;
use op::create_object::OpCreateObjectInput;
use op::create_object::OpCreateObjectOutput;
use op::delete_object::op_delete_object;
use op::delete_object::OpDeleteObjectInput;
use op::delete_object::OpDeleteObjectOutput;
use op::inspect_object::op_inspect_object;
use op::inspect_object::OpInspectObjectInput;
use op::inspect_object::OpInspectObjectOutput;
use op::read_object::op_read_object;
use op::read_object::OpReadObjectInput;
use op::read_object::OpReadObjectOutput;
use op::write_object::op_write_object;
use op::write_object::OpWriteObjectInput;
use op::write_object::OpWriteObjectOutput;
use op::OpResult;
use seekable_async_file::SeekableAsyncFile;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use write_journal::WriteJournal;

pub mod bucket;
pub mod ctx;
pub mod free_list;
pub mod inode;
pub mod object_id;
pub mod op;
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

pub struct BlobdLoader {
  device: SeekableAsyncFile,
  journal: Arc<WriteJournal>,
  bucket_count: u64,

  offsetof_object_id_serial: u64,
  offsetof_stream: u64,
  offsetof_free_list: u64,
  offsetof_buckets: u64,
  reserved_tiles: u32,
  total_tiles: u32,
}

impl BlobdLoader {
  pub fn new(device: SeekableAsyncFile, device_size: u64, bucket_count_log2: u8) -> Self {
    assert!(bucket_count_log2 >= 12 && bucket_count_log2 <= 48);
    let bucket_count = 1u64 << bucket_count_log2;

    let offsetof_journal = 0;
    let sizeof_journal = 1024 * 1024 * 8;
    let offsetof_object_id_serial = offsetof_journal + sizeof_journal;
    let offsetof_stream = offsetof_object_id_serial + 8;
    let offsetof_free_list = offsetof_stream + STREAM_SIZE;
    let offsetof_buckets = offsetof_free_list + FREELIST_SIZE();
    let sizeof_buckets = BUCKETS_SIZE(bucket_count);
    let reserved_space = offsetof_buckets + sizeof_buckets;
    let reserved_tiles: u32 = div_ceil(reserved_space, TILE_SIZE_U64).try_into().unwrap();
    let total_tiles: u32 = (device_size / TILE_SIZE_U64).try_into().unwrap();
    assert!(total_tiles <= FREELIST_TILE_CAP);

    let journal = Arc::new(WriteJournal::new(
      device.clone(),
      offsetof_journal,
      sizeof_journal,
    ));

    Self {
      bucket_count,
      device,
      journal,
      offsetof_buckets,
      offsetof_free_list,
      offsetof_object_id_serial,
      offsetof_stream,
      reserved_tiles,
      total_tiles,
    }
  }

  pub async fn format(&self) {
    let dev = &self.device;
    join! {
      self.journal.format_device(),
      ObjectIdSerial::format_device(dev, self.offsetof_object_id_serial),
      Stream::format_device(dev, self.offsetof_stream),
      FreeList::format_device(dev, self.offsetof_free_list),
      Buckets::format_device(dev, self.offsetof_buckets, self.bucket_count),
    };
    dev.sync_data().await;
  }

  pub async fn load(self) -> Blobd {
    self.journal.recover().await;

    let dev = &self.device;

    // Ensure journal has been recovered first before loading any other data.
    let object_id_serial = ObjectIdSerial::load_from_device(dev, self.offsetof_object_id_serial);
    let stream = Stream::load_from_device(&dev, self.offsetof_stream);
    let free_list = FreeList::load_from_device(
      &dev,
      self.offsetof_free_list,
      self.reserved_tiles,
      self.total_tiles,
    );
    let buckets = Buckets::load_from_device(dev.clone(), self.offsetof_buckets);

    let ctx = Arc::new(Ctx {
      buckets,
      device: dev.clone(),
      free_list: Mutex::new(FreeListWithChangeTracker::new(free_list)),
      journal: Mutex::new(SequentialisedJournal::new(self.journal.clone())),
      object_id_serial,
      stream: RwLock::new(stream),
    });

    Blobd {
      ctx,
      journal: self.journal,
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  ctx: Arc<Ctx>,
  journal: Arc<WriteJournal>,
}

impl Blobd {
  // WARNING: `device.start_delayed_data_sync_background_loop()` must also be running. Since `device` was provided, it's left up to the provider to run it.
  pub async fn start(&self) {
    self.journal.start_commit_background_loop().await;
  }

  pub async fn commit_object(&self, input: OpCommitObjectInput) -> OpResult<OpCommitObjectOutput> {
    op_commit_object(self.ctx.clone(), input).await
  }

  pub async fn create_object(&self, input: OpCreateObjectInput) -> OpResult<OpCreateObjectOutput> {
    op_create_object(self.ctx.clone(), input).await
  }

  pub async fn delete_object(&self, input: OpDeleteObjectInput) -> OpResult<OpDeleteObjectOutput> {
    op_delete_object(self.ctx.clone(), input).await
  }

  pub async fn inspect_object(
    &self,
    input: OpInspectObjectInput,
  ) -> OpResult<OpInspectObjectOutput> {
    op_inspect_object(self.ctx.clone(), input).await
  }

  pub async fn read_object(&self, input: OpReadObjectInput) -> OpResult<OpReadObjectOutput> {
    op_read_object(self.ctx.clone(), input).await
  }

  pub async fn write_object<
    S: Unpin + futures::Stream<Item = Result<Vec<u8>, Box<dyn Error + Send + Sync>>>,
  >(
    &self,
    input: OpWriteObjectInput<S>,
  ) -> OpResult<OpWriteObjectOutput> {
    op_write_object(self.ctx.clone(), input).await
  }
}
