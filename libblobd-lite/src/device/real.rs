use crate::device::IDevice;
use async_trait::async_trait;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;

#[async_trait]
impl IDevice for SeekableAsyncFile {
  async fn sync_data(&self) {
    self.sync_data().await
  }

  async fn write_at_with_delayed_sync(&self, writes: Vec<WriteRequest<Vec<u8>>>) {
    self.write_at_with_delayed_sync(writes).await
  }
}
