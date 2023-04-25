use off64::usz;
use std::collections::VecDeque;
use std::mem::forget;
use std::ops::Deref;
use std::ops::DerefMut;
use std::slice;
use std::sync::Arc;

pub(crate) struct Buf {
  // We use a pointer to avoid complexities and subtleties with dropping when using a Vec<u8> that we also want to move back into the pool on Drop. We currently never free the memory anyway.
  data: *mut u8,
  len: usize,
  #[allow(unused)]
  cap: usize,
  pool: BufPoolForSize,
}

unsafe impl Send for Buf {}
unsafe impl Sync for Buf {}

impl Buf {
  /// This is different from `as_mut_ptr` as it does not do any lifetime checks or require `&mut self`.
  pub fn get_raw_ptr(&self) -> *mut u8 {
    self.data
  }

  pub fn as_slice(&self) -> &[u8] {
    unsafe { slice::from_raw_parts(self.data, self.len) }
  }

  pub fn as_slice_mut(&mut self) -> &mut [u8] {
    unsafe { slice::from_raw_parts_mut(self.data, self.len) }
  }

  pub fn truncate(&mut self, len: usize) {
    assert!(len <= self.len);
    self.len = len;
  }
}

impl AsRef<[u8]> for Buf {
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl AsMut<[u8]> for Buf {
  fn as_mut(&mut self) -> &mut [u8] {
    self.as_slice_mut()
  }
}

impl Deref for Buf {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl DerefMut for Buf {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.as_slice_mut()
  }
}

impl Drop for Buf {
  fn drop(&mut self) {
    self.pool.0.lock().push_back(self.data);
  }
}

#[derive(Clone, Default)]
struct BufPoolForSize(Arc<parking_lot::Mutex<VecDeque<*mut u8>>>);

unsafe impl Send for BufPoolForSize {}
unsafe impl Sync for BufPoolForSize {}

#[derive(Clone)]
pub(crate) struct BufPool {
  sizes: Arc<Vec<BufPoolForSize>>,
}

impl BufPool {
  pub fn new() -> Self {
    let mut sizes = Vec::new();
    for _ in 0..32 {
      sizes.push(Default::default());
    }
    Self {
      sizes: Arc::new(sizes),
    }
  }

  pub fn allocate(&self, len: usize) -> Buf {
    let cap = len.next_power_of_two();
    let pool = self.sizes[usz!(cap.ilog2())].clone();
    // Release lock ASAP.
    let existing = pool.0.lock().pop_front();
    let data = if let Some(data) = existing {
      data
    } else {
      let mut new = vec![0u8; len];
      assert_eq!(new.capacity(), len);
      let data = new.as_mut_ptr();
      forget(new);
      data
    };
    Buf {
      data,
      len,
      cap,
      pool,
    }
  }
}
