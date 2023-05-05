use off64::u8;

// Two benefits of this over Vec<u8>:
// - Length type is u8, which is the type for `tail_page_count`.
// - No heap allocations; small enough to copy.
// How it works:
// - Page sizes can only be from `2^8` to `2^32` (see `{MIN,MAX}_PAGE_SIZE_POW2`).
// - There are only `32 - 8 = 24` page sizes.
// - It takes 5 bits to represent 24 distinct values.
// - There can be up to 24 entries, each taking 5 bits. It will take 5 bits to represent the length.
// - We can't fit 125 bits neatly, so we split up into two shards (64-bit ints), and use 4 bits in each int to represent the length of up to 12 entries covered by its int, using exactly `4 + 12 * 5 = 64` bits.
#[derive(Clone, Copy)]
pub struct TailPageSizes(u64, u64);

impl TailPageSizes {
  pub fn new() -> Self {
    Self(0, 0)
  }

  fn shard_get_len(shard: u64) -> u8 {
    u8!((shard & (0b1111 << 60)) >> 60)
  }

  fn shard_set_len(shard: &mut u64, len: u8) {
    assert!(len <= 12);
    *shard &= !(0b1111 << 60);
    *shard |= u64::from(len) << 60;
  }

  fn shard_get(shard: u64, idx: u8) -> u8 {
    assert!(idx < 12);
    let shift = idx * 5;
    u8!((shard & (0b11111 << shift)) >> shift)
  }

  fn shard_set(shard: &mut u64, idx: u8, val: u8) {
    assert!(val < (1 << 5));
    let shift = idx * 5;
    *shard &= !(0b11111 << shift);
    *shard |= u64::from(val) << shift;
  }

  pub fn len(&self) -> u8 {
    Self::shard_get_len(self.0) + Self::shard_get_len(self.1)
  }

  pub fn get(&self, idx: u8) -> Option<u8> {
    if idx >= self.len() {
      return None;
    };
    Some(if idx < 12 {
      Self::shard_get(self.0, idx)
    } else {
      Self::shard_get(self.1, idx - 12)
    })
  }

  pub fn push(&mut self, pow2: u8) {
    let idx = self.len();
    if idx < 12 {
      Self::shard_set(&mut self.0, idx, pow2);
      Self::shard_set_len(&mut self.0, idx + 1);
    } else {
      Self::shard_set(&mut self.1, idx - 12, pow2);
      Self::shard_set_len(&mut self.1, idx - 12 + 1);
    };
  }
}

impl IntoIterator for TailPageSizes {
  type IntoIter = TailPageSizesIter;
  type Item = (u8, u8);

  fn into_iter(self) -> Self::IntoIter {
    TailPageSizesIter(self, 0)
  }
}

// Convenient iterator that provides `u8` indices instead of `.enumerate()` and `u8!(idx)`.
pub struct TailPageSizesIter(TailPageSizes, u8);

impl Iterator for TailPageSizesIter {
  type Item = (u8, u8);

  fn next(&mut self) -> Option<Self::Item> {
    let idx = self.1;
    let entry = self.0.get(idx)?;
    self.1 += 1;
    Some((idx, entry))
  }
}

#[cfg(test)]
mod tests {
  use super::TailPageSizes;
  use itertools::Itertools;

  #[test]
  fn test_tail_page_sizes() {
    let mut tail = TailPageSizes::new();
    // Check iterator/state is correct when empty.
    assert_eq!(tail.into_iter().collect_vec(), vec![]);

    tail.push(5);
    assert_eq!(tail.into_iter().collect_vec(), vec![(0, 5)]);

    tail.push(3);
    assert_eq!(tail.into_iter().collect_vec(), vec![(0, 5), (1, 3)]);

    tail.push(8);
    assert_eq!(tail.into_iter().collect_vec(), vec![(0, 5), (1, 3), (2, 8)]);
    assert_eq!(tail.get(1), Some(3));
    assert_eq!(tail.get(3), None);

    tail.push(9);
    assert_eq!(tail.into_iter().collect_vec(), vec![
      (0, 5),
      (1, 3),
      (2, 8),
      (3, 9),
    ]);
    assert_eq!(tail.len(), 4);
    assert_eq!(tail.get(0), Some(5));
    assert_eq!(tail.get(4), None);

    tail.push(4);
    assert_eq!(tail.into_iter().collect_vec(), vec![
      (0, 5),
      (1, 3),
      (2, 8),
      (3, 9),
      (4, 4)
    ]);
    assert_eq!(tail.len(), 5);
    assert_eq!(tail.get(3), Some(9));
    assert_eq!(tail.get(5), None);

    tail.push(11);
    assert_eq!(tail.into_iter().collect_vec(), vec![
      (0, 5),
      (1, 3),
      (2, 8),
      (3, 9),
      (4, 4),
      (5, 11),
    ]);
    assert_eq!(tail.len(), 6);
    assert_eq!(tail.get(0), Some(5));
    assert_eq!(tail.get(4), Some(4));
    assert_eq!(tail.get(5), Some(11));
    assert_eq!(tail.get(6), None);

    tail.push(14);
    assert_eq!(tail.into_iter().collect_vec(), vec![
      (0, 5),
      (1, 3),
      (2, 8),
      (3, 9),
      (4, 4),
      (5, 11),
      (6, 14),
    ]);
    assert_eq!(tail.len(), 7);
    assert_eq!(tail.get(0), Some(5));
    assert_eq!(tail.get(4), Some(4));
    assert_eq!(tail.get(5), Some(11));
    assert_eq!(tail.get(6), Some(14));

    tail.push(2);
    assert_eq!(tail.into_iter().collect_vec(), vec![
      (0, 5),
      (1, 3),
      (2, 8),
      (3, 9),
      (4, 4),
      (5, 11),
      (6, 14),
      (7, 2),
    ]);
    assert_eq!(tail.len(), 8);
    assert_eq!(tail.get(0), Some(5));
    assert_eq!(tail.get(4), Some(4));
    assert_eq!(tail.get(5), Some(11));
    assert_eq!(tail.get(6), Some(14));
    assert_eq!(tail.get(7), Some(2));
    assert_eq!(tail.get(8), None);
  }
}
