use super::BackingStore;
use crate::pages::Pages;
use async_trait::async_trait;
use bufpool::buf::Buf;
use std::fs::File;
use std::ops::Deref;
use std::ops::DerefMut;
use uring_file::uring::IoBuf;
use uring_file::uring::IoBufMut;
use uring_file::uring::RegisteredFile;
use uring_file::uring::Uring;
use uring_file::uring::UringCfg;

/// Newtype wrapper to implement uring-file's buffer traits for `bufpool::Buf`.
struct UringBuf(Buf);

impl Deref for UringBuf {
  type Target = [u8];

  fn deref(&self) -> &[u8] {
    &self.0
  }
}

impl DerefMut for UringBuf {
  fn deref_mut(&mut self) -> &mut [u8] {
    &mut self.0
  }
}

// SAFETY: UringBuf wraps Buf which is heap-allocated with stable address.
// The pointer remains valid until dropped, which won't happen during I/O
// since we pass ownership to uring-file and get it back on completion.
unsafe impl IoBuf for UringBuf {
  fn as_ptr(&self) -> *const u8 {
    <[u8]>::as_ptr(&self.0)
  }

  fn len(&self) -> usize {
    <[u8]>::len(&self.0)
  }
}

// SAFETY: Same as IoBuf - Buf is heap-allocated with stable address.
unsafe impl IoBufMut for UringBuf {
  fn as_mut_ptr(&mut self) -> *mut u8 {
    <[u8]>::as_mut_ptr(&mut self.0)
  }

  fn capacity(&self) -> usize {
    // Buf is allocated to exact size, capacity == len
    <[u8]>::len(&self.0)
  }
}

pub(crate) struct UringBackingStore {
  pages: Pages,
  uring: Uring,
  file: RegisteredFile,
}

impl UringBackingStore {
  pub fn new(file: File, pages: Pages, cfg: UringCfg) -> Self {
    let uring = Uring::new(cfg).expect("failed to create io_uring");
    let registered_file = uring
      .register(&file)
      .expect("failed to register file with io_uring");

    Self {
      pages,
      uring,
      file: registered_file,
    }
  }
}

#[async_trait]
impl BackingStore for UringBackingStore {
  async fn read_at(&self, offset: u64, len: u64) -> Buf {
    let buf = UringBuf(self.pages.allocate_uninitialised(len));
    let result = self
      .uring
      .read_into(&self.file, offset, buf)
      .await
      .expect("io_uring read failed");
    assert_eq!(
      result.bytes_read, len as usize,
      "short read: expected {} bytes, got {}",
      len, result.bytes_read
    );
    result.buf.0
  }

  async fn write_at(&self, offset: u64, data: Buf) -> Buf {
    let data_len = data.len();
    let result = self
      .uring
      .write_at(&self.file, offset, UringBuf(data))
      .await
      .expect("io_uring write failed");
    assert_eq!(
      result.bytes_written, data_len,
      "short write: expected {} bytes, wrote {}",
      data_len, result.bytes_written
    );
    result.buf.0
  }

  async fn sync(&self) {
    self
      .uring
      .sync(&self.file)
      .await
      .expect("io_uring sync failed");
  }
}
