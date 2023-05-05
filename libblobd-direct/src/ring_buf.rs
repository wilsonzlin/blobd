use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use rustc_hash::FxHashSet;
use std::ops::Deref;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct RingBufItem {
  pub virtual_offset: usize,
  pub len: usize,
}

pub(crate) enum BufOrSlice<'a> {
  Buf(Buf),
  Slice(&'a [u8]),
}

impl<'a> Deref for BufOrSlice<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      BufOrSlice::Buf(b) => b.as_slice(),
      BufOrSlice::Slice(s) => s,
    }
  }
}

/// A persisted contiguous block of bytes that will discard older entries as newer entries are pushed, where an entry is any arbitrary slice of bytes.
/// Every entry pushed is represented by a virtual offset and length, and are pushed at the `virtual_tail` position. Once resolved to the physical position, the pushed data may overwrite/clobber part or all of older entries. **This is not managed by RingBuf**, and it's up to the caller to decide the best way to track and manage this. Also, because of this, RingBuf has no way of storing the virtual head (since `virtual_tail - capacity` may not be exactly at an entry boundary), so this is also left up to the user.
/// Essentially, use this as a way to store and retrieve bytes compactly (including across page boundaries and rotated around the ring), and to determine which pages are dirty.
pub(crate) struct RingBuf {
  // TODO A virtual uncommitted head offset would probably suffice and be faster, although slightly more complex.
  dirty_physical_pages: FxHashSet<usize>,
  page_size: usize,
  raw: Vec<u8>,
  virtual_tail: usize,
}

impl RingBuf {
  pub fn new(cap: usize, page_size: usize) -> Self {
    assert_eq!(cap % page_size, 0);
    Self {
      dirty_physical_pages: FxHashSet::default(),
      page_size,
      raw: vec![0u8; cap],
      virtual_tail: 0,
    }
  }

  pub fn load(page_size: usize, virtual_tail: usize, raw: Vec<u8>) -> Self {
    assert_eq!(raw.len() % page_size, 0);
    Self {
      dirty_physical_pages: FxHashSet::default(),
      page_size,
      raw,
      virtual_tail,
    }
  }

  pub fn capacity(&self) -> usize {
    self.raw.len()
  }

  pub fn push(&mut self, data: &[u8]) -> RingBufItem {
    assert!(data.len() < self.capacity());
    let virtual_offset = self.virtual_tail;
    for virtual_offset in (virtual_offset..virtual_offset + data.len()).step_by(self.page_size) {
      self
        .dirty_physical_pages
        .insert(virtual_offset % self.capacity());
    }

    let physical_offset = virtual_offset % self.capacity();
    if physical_offset + data.len() > self.capacity() {
      let (a, b) = data.split_at(self.capacity() - physical_offset);
      self.raw[physical_offset..].copy_from_slice(a);
      self.raw[..b.len()].copy_from_slice(b);
    } else {
      self.raw[physical_offset..physical_offset + data.len()].copy_from_slice(data);
    };
    RingBufItem {
      virtual_offset,
      len: data.len(),
    }
  }

  pub fn get<'a>(&'a self, item: RingBufItem) -> Option<BufOrSlice<'a>> {
    if self.virtual_tail.saturating_sub(self.capacity()) > item.virtual_offset {
      return None;
    };
    let physical_offset = item.virtual_offset % self.capacity();
    if physical_offset + item.len > self.capacity() {
      let mut buf = BUFPOOL.allocate(item.len);
      buf.extend_from_slice(&self.raw[physical_offset..]);
      buf.extend_from_slice(&self.raw[..item.len - buf.len()]);
      Some(BufOrSlice::Buf(buf))
    } else {
      Some(BufOrSlice::Slice(
        &self.raw[physical_offset..physical_offset + item.len],
      ))
    }
  }

  pub fn virtual_tail(&self) -> usize {
    self.virtual_tail
  }

  pub fn commit(&mut self) -> Option<impl Iterator<Item = (usize, &[u8])>> {
    if self.dirty_physical_pages.is_empty() {
      return None;
    };
    Some(
      self
        .dirty_physical_pages
        .drain()
        .into_iter()
        .map(|physical_offset| {
          (
            physical_offset,
            &self.raw[physical_offset..physical_offset + self.page_size],
          )
        }),
    )
  }
}
