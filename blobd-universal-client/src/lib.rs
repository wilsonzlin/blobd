use async_trait::async_trait;
use futures::Stream;
use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub mod direct;
pub mod kv;
pub mod lite;

pub struct InitCfgPartition {
  pub path: PathBuf,
  pub offset: u64,
  pub len: u64,
}

pub struct InitCfg {
  pub bucket_count: u64,
  pub do_not_format_device: bool,
  pub log_buffer_size: u64,
  pub lpage_size: u64,
  pub object_count: u64,
  pub partitions: Vec<InitCfgPartition>,
  pub spage_size: u64,
}

pub type IncompleteToken = Arc<dyn Any + Send + Sync>;

pub struct CreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
}

pub struct CreateObjectOutput {
  pub token: IncompleteToken,
}

pub struct WriteObjectInput<'a> {
  pub key: TinyBuf,
  pub offset: u64,
  pub incomplete_token: IncompleteToken,
  pub data: &'a [u8],
}

pub struct CommitObjectInput {
  pub incomplete_token: IncompleteToken,
}

pub struct CommitObjectOutput {
  pub object_id: u64,
}

pub struct InspectObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
}

pub struct InspectObjectOutput {
  pub id: u64,
  pub size: u64,
}

pub struct ReadObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
  pub start: u64,
  pub end: Option<u64>,
  pub stream_buffer_size: u64,
}

pub struct ReadObjectOutput {
  pub data_stream: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
}

pub struct DeleteObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
}

#[async_trait]
pub trait BlobdProvider: Send + Sync {
  fn metrics(&self) -> Vec<(&'static str, u64)>;
  async fn wait_for_end(&self);
  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput;
  async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>);
  async fn commit_object(&self, input: CommitObjectInput) -> CommitObjectOutput;
  async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput;
  async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput;
  async fn delete_object(&self, input: DeleteObjectInput);
}
