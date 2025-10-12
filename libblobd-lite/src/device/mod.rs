use async_trait::async_trait;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use seekable_async_file::WriteRequest;

pub mod mock;
pub mod real;

#[async_trait]
pub trait IDevice:
  for<'a> Off64AsyncRead<'a, Vec<u8>>
  + for<'a> Off64AsyncReadInt<'a, Vec<u8>>
  + Off64AsyncWrite
  + Off64AsyncWriteInt
  + Send
  + Sync
{
  async fn sync_data(&self);

  async fn write_at_with_delayed_sync(&self, writes: Vec<WriteRequest<Vec<u8>>>);
}
