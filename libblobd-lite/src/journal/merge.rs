use off64::u64;
use off64::usz;
use std::collections::BTreeMap;

/// A data structure that tracks write requests and merges overlapping writes.
/// Later writes win over earlier writes at overlaps.
#[derive(Debug, Clone)]
pub(crate) struct WriteMerger {
  // Map from offset to data
  // Invariant: all entries are non-overlapping
  writes: BTreeMap<u64, Vec<u8>>,
}

impl WriteMerger {
  /// Create a new empty WriteMerger
  pub fn new() -> Self {
    Self {
      writes: BTreeMap::new(),
    }
  }

  /// Insert a write at the given offset with the given data.
  /// Merges with any overlapping writes, with this write winning at overlaps.
  pub fn insert(&mut self, offset: u64, data: Vec<u8>) {
    if data.is_empty() {
      return;
    }

    let end = offset + u64!(data.len());

    // Collect all overlapping entries and their preserved parts
    let mut to_preserve = Vec::new();
    let mut to_remove = Vec::new();

    // Find all entries that overlap with [offset, end)
    for (&existing_offset, existing_data) in self.writes.iter() {
      let existing_end = existing_offset + u64!(existing_data.len());

      if existing_end <= offset {
        // Existing write ends before new write starts, no overlap
        continue;
      }
      if existing_offset >= end {
        // Existing write starts after new write ends, no more overlaps possible
        break;
      }

      // There's an overlap, mark for removal
      to_remove.push(existing_offset);

      // If existing write extends before our new write, preserve that prefix
      if existing_offset < offset {
        let prefix_len = usz!(offset - existing_offset);
        let prefix = existing_data[..prefix_len].to_vec();
        to_preserve.push((existing_offset, prefix));
      }

      // If existing write extends after our new write, preserve that suffix
      if existing_end > end {
        let suffix_start = usz!(end - existing_offset);
        let suffix = existing_data[suffix_start..].to_vec();
        to_preserve.push((end, suffix));
      }
    }

    // Remove all overlapping entries
    for offset_to_remove in to_remove {
      self.writes.remove(&offset_to_remove);
    }

    // Insert preserved parts
    for (pres_offset, pres_data) in to_preserve {
      self.writes.insert(pres_offset, pres_data);
    }

    // Insert the new write
    self.writes.insert(offset, data);
  }

  /// Consume the merger and return an iterator of non-overlapping (offset, data) pairs.
  /// The writes are returned in offset order.
  pub fn into_merged(self) -> BTreeMap<u64, Vec<u8>> {
    self.writes
  }
}

impl Default for WriteMerger {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_non_overlapping_writes() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3]);
    merger.insert(10, vec![4, 5, 6]);
    merger.insert(20, vec![7, 8, 9]);

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0], (0, vec![1, 2, 3]));
    assert_eq!(writes[1], (10, vec![4, 5, 6]));
    assert_eq!(writes[2], (20, vec![7, 8, 9]));
  }

  #[test]
  fn test_complete_overlap() {
    let mut merger = WriteMerger::new();
    merger.insert(10, vec![1, 2, 3, 4, 5]);
    merger.insert(10, vec![6, 7, 8, 9, 10]);

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0], (10, vec![6, 7, 8, 9, 10]));
  }

  #[test]
  fn test_partial_overlap_later_wins() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3, 4, 5]);
    merger.insert(3, vec![9, 9, 9]);

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0], (0, vec![1, 2, 3]));
    assert_eq!(writes[1], (3, vec![9, 9, 9]));
  }

  #[test]
  fn test_new_write_covers_middle() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3, 4, 5]);
    merger.insert(1, vec![9, 9, 9]);

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0], (0, vec![1]));
    assert_eq!(writes[1], (1, vec![9, 9, 9]));
    assert_eq!(writes[2], (4, vec![5]));
  }

  #[test]
  fn test_new_write_spans_multiple() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3]);
    merger.insert(10, vec![4, 5, 6]);
    merger.insert(20, vec![7, 8, 9]);
    merger.insert(5, vec![99; 20]); // Covers [5, 25), overlaps [10,13) and [20,23)

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0], (0, vec![1, 2, 3])); // [0, 3) preserved
    assert_eq!(writes[1], (5, vec![99; 20])); // [5, 25) new write
  }

  #[test]
  fn test_empty_data() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3]);
    merger.insert(10, vec![]); // Empty should be ignored

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0], (0, vec![1, 2, 3]));
  }

  #[test]
  fn test_adjacent_writes() {
    let mut merger = WriteMerger::new();
    merger.insert(0, vec![1, 2, 3]);
    merger.insert(3, vec![4, 5, 6]);
    merger.insert(6, vec![7, 8, 9]);

    let writes: Vec<_> = merger.into_merged().into_iter().collect();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0], (0, vec![1, 2, 3]));
    assert_eq!(writes[1], (3, vec![4, 5, 6]));
    assert_eq!(writes[2], (6, vec![7, 8, 9]));
  }
}
