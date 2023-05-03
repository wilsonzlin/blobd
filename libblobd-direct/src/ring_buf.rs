use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use rustc_hash::FxHashSet;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct RingBufItem {
  pub virtual_offset: usize,
  pub len: usize,
}

pub(crate) enum BufOrSlice<'a> {
  Buf(Buf),
  Slice(&'a [u8]),
}

impl<'a> std::ops::Deref for BufOrSlice<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      BufOrSlice::Buf(b) => b.as_slice(),
      BufOrSlice::Slice(s) => s,
    }
  }
}

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

  pub fn push(&mut self, data: &[u8]) -> RingBufItem {
    assert!(data.len() < self.raw.len());
    let virtual_offset = self.virtual_tail;
    for virtual_offset in (virtual_offset..virtual_offset + data.len()).step_by(self.page_size) {
      self
        .dirty_physical_pages
        .insert(virtual_offset % self.raw.len());
    }

    let physical_offset = virtual_offset % self.raw.len();
    if physical_offset + data.len() > self.raw.len() {
      let (a, b) = data.split_at(self.raw.len() - physical_offset);
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
    if self.virtual_tail.saturating_sub(self.raw.len()) > item.virtual_offset {
      return None;
    };
    let physical_offset = item.virtual_offset % self.raw.len();
    if physical_offset + item.len > self.raw.len() {
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

  pub fn get_virtual_tail(&self) -> usize {
    self.virtual_tail
  }

  pub fn commit(&mut self) -> impl Iterator<Item = (usize, &[u8])> {
    self
      .dirty_physical_pages
      .drain()
      .into_iter()
      .map(|physical_offset| {
        (
          physical_offset,
          &self.raw[physical_offset..physical_offset + self.page_size],
        )
      })
  }
}
