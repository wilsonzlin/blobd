use async_trait::async_trait;
use futures::Stream;
use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tinybuf::TinyBuf;

pub mod direct;
pub mod vanilla;

pub struct InitCfg {
  pub bucket_count: u64,
  pub device_size: u64,
  pub device: PathBuf,
  pub lpage_size: u64,
  pub object_count: u64,
  pub spage_size: u64,
}

pub type TargetIncompleteToken = Arc<dyn Any + Send + Sync>;

pub struct TargetCreateObjectInput {
  pub key: TinyBuf,
  pub size: u64,
  pub assoc_data: TinyBuf,
}

pub struct TargetCreateObjectOutput {
  pub token: TargetIncompleteToken,
}

pub struct TargetWriteObjectInput<'a> {
  pub offset: u64,
  pub incomplete_token: TargetIncompleteToken,
  pub data: &'a [u8],
}

pub struct TargetCommitObjectInput {
  pub incomplete_token: TargetIncompleteToken,
}

pub struct TargetCommitObjectOutput {
  pub object_id: u64,
}

pub struct TargetInspectObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
}

pub struct TargetInspectObjectOutput {
  pub id: u64,
  pub size: u64,
}

pub struct TargetReadObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
  pub start: u64,
  pub end: Option<u64>,
  pub stream_buffer_size: u64,
}

pub struct TargetReadObjectOutput {
  pub data_stream: Pin<Box<dyn Stream<Item = Box<dyn AsRef<[u8]>>> + Send>>,
}

pub struct TargetDeleteObjectInput {
  pub key: TinyBuf,
  pub id: Option<u64>,
}

#[async_trait]
pub trait Target: Clone {
  async fn start(cfg: InitCfg, completed: Arc<AtomicU64>) -> Self;
  async fn create_object(&self, input: TargetCreateObjectInput) -> TargetCreateObjectOutput;
  async fn write_object<'a>(&'a self, input: TargetWriteObjectInput<'a>);
  async fn commit_object(&self, input: TargetCommitObjectInput) -> TargetCommitObjectOutput;
  async fn inspect_object(&self, input: TargetInspectObjectInput) -> TargetInspectObjectOutput;
  async fn read_object(&self, input: TargetReadObjectInput) -> TargetReadObjectOutput;
  async fn delete_object(&self, input: TargetDeleteObjectInput);
}
