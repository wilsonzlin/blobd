use crate::journal::Transaction;
use crate::uring::Uring;
use crate::util::div_pow2;
use bufpool::BUFPOOL;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::usz;
use rustc_hash::FxHashSet;

// One container uses one spage on the device as backing persistent storage.
struct BitmapContainer {
  dev_offset: u64,
  next_dev_offset: u64,
  bitmap: Vec<u64>,
  // Indices of elements in `bitmap` that have at least one set bit.
  free_elems: FxHashSet<usize>,
}

impl BitmapContainer {
  fn deserialise(dev_offset: u64, raw: &[u8]) -> Self {
    // spage must be multiple of 8.
    assert_eq!(raw.len() % 8, 0);
    let cap = (raw.len() - 8) / 8;
    let mut bitmap = Vec::with_capacity(cap);
    let mut free_elems = FxHashSet::default();
    for i in 0..cap {
      let elem = raw.read_u64_le_at(u64!(i) * 8);
      bitmap.push(elem);
      if elem != 0 {
        free_elems.insert(i);
      }
    }
    let next_dev_offset = raw.read_u64_le_at(u64!(cap) * 8);
    Self {
      dev_offset,
      next_dev_offset,
      bitmap,
      free_elems,
    }
  }

  fn serialise(&self, out: &mut [u8]) {
    assert_eq!(out.len(), self.bitmap.len() * 8 + 8);
    for (i, elem) in self.bitmap.iter().enumerate() {
      out.write_u64_le_at(u64!(i) * 8, *elem);
    }
    out.write_u64_le_at(u64!(self.bitmap.len()) * 8, self.next_dev_offset);
  }

  fn idx_pos(num: u64) -> (usize, u64) {
    let idx = usz!(num / 64);
    let pos = num % 64;
    (idx, pos)
  }

  fn mark_as_free(&mut self, num: u64) {
    let (idx, pos) = Self::idx_pos(num);
    self.bitmap[usz!(idx)] |= 1 << pos;
    self.free_elems.insert(usz!(idx));
  }

  fn mark_as_used(&mut self, num: u64) {
    let (idx, pos) = Self::idx_pos(num);
    let elem = &mut self.bitmap[idx];
    *elem &= !(1 << pos);
    if *elem == 0 {
      self.free_elems.remove(&idx);
    };
  }

  fn check_is_free(&self, num: u64) -> bool {
    let (idx, pos) = Self::idx_pos(num);
    0 != (self.bitmap[idx] & (1 << pos))
  }

  fn remove_any_free(&mut self) -> u64 {
    let idx = *self.free_elems.iter().next().unwrap();
    let elem = &mut self.bitmap[idx];
    let pos = elem.trailing_zeros();
    assert!(pos < 64);
    *elem &= !(1 << pos);
    if *elem == 0 {
      self.free_elems.remove(&idx);
    };
    u64!(*elem) * 64 + u64!(pos)
  }

  fn has_free(&self) -> bool {
    !self.free_elems.is_empty()
  }
}

pub(super) struct BitmapContainers {
  page_size_pow2: u8,
  heap_dev_offset: u64,
  // How many bits one container represents.
  container_len: u64,
  containers: Vec<BitmapContainer>,
  // Indices of elements in `containers` that require committing to device.
  dirty: FxHashSet<usize>,
  // Indices of elements in `containers` that have at least one set bit.
  free: FxHashSet<usize>,
}

impl BitmapContainers {
  pub async fn load_from_device(
    dev: Uring,
    spage_size: u64,
    page_size_pow2: u8,
    heap_dev_offset: u64,
    first_container_dev_offset: u64,
  ) -> Self {
    let mut dev_offset = first_container_dev_offset;
    let mut containers = Vec::new();
    let mut free = FxHashSet::default();
    while dev_offset != 0 {
      let idx = containers.len();
      let buf = dev.read(dev_offset, spage_size).await;
      let container = BitmapContainer::deserialise(dev_offset, buf.as_slice());
      if container.has_free() {
        free.insert(idx);
      };
      dev_offset = container.next_dev_offset;
      containers.push(container);
    }
    Self {
      container_len: (u64!(spage_size) - 8) * 8,
      containers,
      dirty: Default::default(),
      free,
      heap_dev_offset,
      page_size_pow2,
    }
  }

  pub fn commit(&mut self, txn: &mut Transaction) {
    for idx in self.dirty.drain() {
      let mut buf = BUFPOOL.allocate_with_zeros(1 << self.page_size_pow2);
      self.containers[idx].serialise(&mut buf);
      txn.record(self.containers[idx].dev_offset, buf);
    }
  }

  fn idx_pos(&self, page_dev_offset: u64) -> (usize, u64) {
    let page_num = div_pow2(page_dev_offset - self.heap_dev_offset, self.page_size_pow2);
    let idx = usz!(page_num / self.container_len);
    let pos = page_num % self.container_len;
    (idx, pos)
  }

  pub fn check_if_page_is_free(&self, page_dev_offset: u64) -> bool {
    let (idx, pos) = self.idx_pos(page_dev_offset);
    self.containers[idx].check_is_free(pos)
  }

  /// Removes a free page and returns its number.
  pub fn pop_free_page(&mut self) -> Option<u64> {
    let idx = *self.free.iter().next()?;
    let pos = self.containers[idx].remove_any_free();
    self.dirty.insert(idx);
    if !self.containers[idx].has_free() {
      self.free.remove(&idx);
    };
    Some(u64!(idx) * self.container_len + pos)
  }

  pub fn mark_page_as_free(&mut self, page_dev_offset: u64) {
    let (idx, pos) = self.idx_pos(page_dev_offset);
    self.containers[idx].mark_as_used(pos);
    self.dirty.insert(idx);
    self.free.insert(idx);
  }

  pub fn mark_page_as_not_free(&mut self, page_dev_offset: u64) {
    let (idx, pos) = self.idx_pos(page_dev_offset);
    self.containers[idx].mark_as_free(pos);
    self.dirty.insert(idx);
    if !self.containers[idx].has_free() {
      self.free.remove(&idx);
    };
  }
}
