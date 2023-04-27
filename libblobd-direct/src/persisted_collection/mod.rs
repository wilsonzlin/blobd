pub mod btree;
pub mod dashmap;
pub mod hashmap;
pub mod queue;
pub mod serialisable;
pub mod vec;

use self::serialisable::Serialisable;
use self::serialisable::SerialisedLen;
use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::uring::Uring;
use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use futures::Stream;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::usz;
use off64::Off64Read;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;

/*

Structure
---------

u40 next_dev_offset_rshift8_or_zero
u24 len_of_serialised_data_in_bytes
u8[] serialised_data

*/

const PAGE_OFFSETOF_NEXT_DEV_OFFSET_RSHIFT8: u64 = 0;
const PAGE_OFFSETOF_LEN: u64 = PAGE_OFFSETOF_NEXT_DEV_OFFSET_RSHIFT8 + 5;
const PAGE_OFFSETOF_DATA: u64 = PAGE_OFFSETOF_LEN + 3;
const PAGE_RESERVED_BYTES: u64 = PAGE_OFFSETOF_DATA;

struct EntryReader<'a, K: Serialisable, V: Serialisable> {
  raw_page: &'a [u8],
  next: u64,
  end: u64,
  key_deserialise_args: &'a K::DeserialiseArgs,
  value_deserialise_args: &'a V::DeserialiseArgs,
  _phantom: PhantomData<(K, V)>,
}

impl<'a, K: Serialisable, V: Serialisable> EntryReader<'a, K, V> {
  pub fn new(
    raw_page: &'a [u8],
    len: u64,
    key_deserialise_args: &'a K::DeserialiseArgs,
    value_deserialise_args: &'a V::DeserialiseArgs,
  ) -> Self {
    Self {
      raw_page,
      next: PAGE_RESERVED_BYTES,
      end: PAGE_RESERVED_BYTES + len,
      key_deserialise_args,
      value_deserialise_args,
      _phantom: PhantomData,
    }
  }

  fn _read_len<T: Serialisable>(&mut self) -> SerialisedLen {
    match T::FIXED_SERIALISED_LEN {
      Some(fl) => fl,
      None => {
        let l = self.raw_page.read_u16_le_at(self.next);
        self.next += 2;
        l
      }
    }
  }
}

impl<'a, K: Serialisable, V: Serialisable> Iterator for EntryReader<'a, K, V> {
  type Item = (K, V);

  fn next(&mut self) -> Option<Self::Item> {
    if self.next == self.end {
      return None;
    };
    assert!(self.next < self.end);

    let key_len = u64!(self._read_len::<K>());
    let key_raw = self.raw_page.read_at(self.next, key_len);
    self.next += key_len;
    let key = K::deserialise(self.key_deserialise_args, key_raw);

    let value_len = u64!(self._read_len::<V>());
    let value_raw = self.raw_page.read_at(self.next, value_len);
    self.next += value_len;
    let value = V::deserialise(self.value_deserialise_args, value_raw);

    Some((key, value))
  }
}

fn serialise_entry<K: Serialisable, V: Serialisable>(spage_size: u64, k: K, v: &V) -> Buf {
  let mut out = BUFPOOL.allocate(usz!(spage_size));

  let key_len = k.serialised_len();
  if K::FIXED_SERIALISED_LEN.is_none() {
    out.write_u16::<LittleEndian>(key_len).unwrap();
  }
  let mut key_raw = BUFPOOL.allocate_with_zeros(usz!(key_len));
  k.serialise(&mut key_raw);
  out.append(&mut key_raw);

  let value_len = v.serialised_len();
  if V::FIXED_SERIALISED_LEN.is_none() {
    out.write_u16::<LittleEndian>(value_len).unwrap();
  }
  let mut value_raw = BUFPOOL.allocate_with_zeros(usz!(value_len));
  v.serialise(&mut value_raw);
  out.append(&mut value_raw);

  out
}

struct Page<K: Eq + Hash> {
  entries: FxHashSet<K>,
  free: u64,
  next_dev_offset: Option<u64>,
}

/// For performance, when entries are inserted/updated/removed, their key is simply marked as dirty, delaying the page shuffles until commit time. This is faster because it coalesces updates, and opportunistically performs calculations during slack time (which is probably when commits happen).
struct CollectionState<K: Copy + Eq + Hash + Serialisable, V: Serialisable> {
  spage_size: u64,
  // The key is the page's device offset.
  pages: FxHashMap<u64, Page<K>>,
  // key => page_dev_offset. The value is zero if it hasn't been assigned to any page yet; using Option<u64> would double our memory usage.
  key_to_page: FxHashMap<K, u64>,
  // (free_space, page_dev_offset).
  by_free_space: BTreeSet<(u64, u64)>,
  dirty: FxHashSet<K>,
  _phantom: PhantomData<V>,
}

impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> CollectionState<K, V> {
  fn entry_was_updated(&mut self, k: K) {
    self.dirty.insert(k);
  }

  fn entry_was_inserted(&mut self, k: K) {
    let None = self.key_to_page.insert(k, 0) else {
      unreachable!();
    };
    self.dirty.insert(k);
  }

  fn entry_was_removed(&mut self, k: K) {
    let page = self.key_to_page.remove(&k).unwrap();
    assert!(self.pages.get_mut(&page).unwrap().entries.remove(&k));
    self.dirty.insert(k);
  }

  fn new(spage_size: u64) -> Self {
    Self {
      spage_size,
      pages: Default::default(),
      key_to_page: Default::default(),
      by_free_space: Default::default(),
      dirty: Default::default(),
      _phantom: Default::default(),
    }
  }

  fn load_from_device<'a>(
    &'a mut self,
    dev: Uring,
    first_page_dev_offset: u64,
    key_deserialise_args: &'a K::DeserialiseArgs,
    value_deserialise_args: &'a V::DeserialiseArgs,
  ) -> impl Stream<Item = (K, V)> + 'a {
    async_stream::stream! {
      let mut dev_offset = first_page_dev_offset;
      while dev_offset != 0 {
        let raw = dev.read(dev_offset, self.spage_size).await;

        let len = u64!(raw.read_u24_le_at(PAGE_OFFSETOF_LEN));
        let next_dev_offset = raw.read_u40_le_at(PAGE_OFFSETOF_NEXT_DEV_OFFSET_RSHIFT8) >> 8;

        let mut page = Page {
          entries: Default::default(),
          free: self.spage_size - PAGE_RESERVED_BYTES - len,
          next_dev_offset: Some(next_dev_offset).filter(|o| *o != 0),
        };
        self.by_free_space.insert((page.free, dev_offset));

        for (key, value) in EntryReader::new(&raw, len, key_deserialise_args, value_deserialise_args) {
          page.entries.insert(key);
          let None = self.key_to_page.insert(key, dev_offset) else {
            unreachable!();
          };
          yield (key, value);
        };

        let None = self.pages.insert(dev_offset, page) else {
          unreachable!();
        };
        dev_offset = next_dev_offset;
      };
    }
  }

  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    dev
      .write(
        first_page_dev_offset,
        BUFPOOL.allocate_with_zeros(usz!(spage_size)),
      )
      .await;
  }

  fn commit<'a, FnV: Deref<Target = V> + 'a>(
    &'a mut self,
    txn: &mut Transaction,
    alloc: &mut Allocator,
    getter: impl Fn(K) -> Option<FnV>,
  ) {
    if self.dirty.is_empty() {
      return;
    };

    // Our first priority is to minimise writes. We can't write any less than 1 spage and we can only write data in multiples of the spage size, so our only variable is how many spages we write.
    // Our second priority is to minimise unused space. However, this is below the priority to minimise writes; after all, the best way to optimise unused space is to bin pack and rewrite everything everytime, but this would be extremely wasteful with writes.
    // Our third priority is to minimise CPU time. We perform commits in a tight slack-free synchronous state worker loop, so they will block everything while they're running.

    // TODO Implement special optimised branch for fixed keys and values, which isn't a bin packing problem.

    // NOTE: Bin packing is NP-hard. It's also impossible to know what's optimal given that that depends on future actions; for example, even an inefficient packing of entries now (e.g. wastes a lot of space) may turn out to be perfect and optimal for the next future commit.

    /// This holds page data in memory, designed to build a new or replacement page. It contains an exact-sized buffer that matches the exact bytes that should be written to/exist on the device. **Make sure to set the metadata stored in the reserved space.**
    /// This is ordered by (i.e. implements `Ord` based on) free space.
    struct PageData<K, V> {
      dev_offset: u64,
      raw: Buf,
      entries: FxHashSet<K>,
      _phantom: PhantomData<V>,
    }
    impl<K, V> PageData<K, V> {
      fn free(&self) -> u64 {
        u64!(self.raw.capacity() - self.raw.len())
      }
    }
    impl<K: Copy + Eq + Hash + Serialisable, V: Serialisable> PageData<K, V> {
      fn new(spage_size: u64, dev_offset: u64) -> Self {
        let mut raw = BUFPOOL.allocate(usz!(spage_size));
        raw.extend_from_slice(&[0u8; PAGE_RESERVED_BYTES as usize]);
        PageData {
          dev_offset,
          raw,
          entries: FxHashSet::default(),
          _phantom: PhantomData,
        }
      }

      fn write_next_dev_offset(&mut self, next_dev_offset: u64) {
        self
          .raw
          .write_u40_le_at(PAGE_OFFSETOF_NEXT_DEV_OFFSET_RSHIFT8, next_dev_offset << 8);
      }

      fn maybe_write_raw(&mut self, serialised_entry: &[u8]) -> Option<()> {
        if self.raw.capacity() - self.raw.len() >= serialised_entry.len() {
          self.raw.extend_from_slice(&serialised_entry);
          Some(())
        } else {
          None
        }
      }

      fn maybe_write(&mut self, k: K, v: &V) -> Option<()> {
        assert!(self.entries.insert(k));
        self.maybe_write_raw(&serialise_entry(u64!(self.raw.capacity()), k, v))
      }
    }
    impl<K, V> PartialEq for PageData<K, V> {
      fn eq(&self, other: &Self) -> bool {
        self.free() == other.free()
      }
    }
    impl<K, V> Eq for PageData<K, V> {}
    impl<K, V> PartialOrd for PageData<K, V> {
      fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.free().partial_cmp(&other.free())
      }
    }
    impl<K, V> Ord for PageData<K, V> {
      fn cmp(&self, other: &Self) -> Ordering {
        self.free().cmp(&other.free())
      }
    }

    // Use a set as we may see key again when extending from a page's entries.
    let mut keys_to_write = FxHashSet::default();
    // These pages contain entries that have been updated or deleted, so they must be overwritten or released (otherwise they will contain stale incorrect data), so we should always use them first before inserting into other existing non-dirty pages or allocating new pages.
    let mut dirty_page_dev_offsets = FxHashSet::default();
    for k in self.dirty.drain() {
      // Even if key doesn't exist, page still needs to be written. We'll check entry exists for key later (when we get the entry) to avoid double lookup.
      keys_to_write.insert(k);
      let page_dev_offset = *self.key_to_page.get(&k).unwrap();
      if page_dev_offset != 0 {
        if !dirty_page_dev_offsets.insert(page_dev_offset) {
          let p = self.pages.get(&page_dev_offset).unwrap();
          keys_to_write.extend(p.entries.iter());
          // Remove from `by_free_space` so we don't accidentally pick them later.
          assert!(self.by_free_space.remove(&(p.free, page_dev_offset)));
        };
      };
    }
    // If we have zero dirty pages, it means we're only inserting new entries. On the off chance that we will overflow and only allocate new pages later on instead of reusing any existing non-dirty page, we won't be able to insert the new pages into the linked list as we're not updating any existing page. Therefore, we'll force a page to be dirty now.
    if dirty_page_dev_offsets.is_empty() {
      let (_, page_dev_offset) = self.by_free_space.pop_last().unwrap();
      let p = self.pages.get(&page_dev_offset).unwrap();
      keys_to_write.extend(p.entries.iter());
    };

    let mut dirty_page_replacements = BinaryHeap::<PageData<K, V>>::new();
    for page_dev_offset in dirty_page_dev_offsets.iter() {
      dirty_page_replacements.push(PageData::new(self.spage_size, *page_dev_offset));
    }
    let mut overflow = Vec::new();
    for &k in keys_to_write.iter() {
      let Some(v) = getter(k) else {
        continue;
      };
      #[rustfmt::skip]
      let serialised_len = u64!(
        if K::FIXED_SERIALISED_LEN.is_some() { 0 } else { 2 }
        + k.serialised_len()
        + if V::FIXED_SERIALISED_LEN.is_some() { 0 } else { 2 }
        + v.deref().serialised_len()
      );
      if dirty_page_replacements
        .peek()
        .filter(|s| s.free() >= serialised_len)
        .is_some()
      {
        let mut page_data = dirty_page_replacements.pop().unwrap();
        page_data.maybe_write(k, v.deref()).unwrap();
        dirty_page_replacements.push(page_data);
      } else {
        // This cannot fit in any dirty page.
        overflow.push(serialise_entry(self.spage_size, k, v.deref()));
      };
    }

    // These entries couldn't fit on any dirty page.
    // We'll try to fit some on existing non-dirty pages if optimal. We can't just try and cram into as many existing pages as possible as that would significantly increase writes and CPU usage.
    // We fill from smallest to biggest, as otherwise we could be blocked unable to fill a large entry despite plenty of room for the other remaining smaller ones.
    overflow.sort_unstable_by_key(|o| o.len());
    let mut overflow_rem_len: u64 = overflow.iter().map(|o| u64!(o.len())).sum();
    let mut overflow = VecDeque::from(overflow);
    // (page_data, original_free_space).
    let mut overflow_page_replacements = Vec::new();
    for &(free, page_dev_offset) in self.by_free_space.iter().rev() {
      let Some(o) = overflow.front() else {
        break;
      };
      // This page doesn't have enough to fit `o`, and all following `o` values are even larger, so we've reached the end condition.
      if free < u64!(o.len()) {
        break;
      };
      // If we still have more than one page amount of remaining bytes: we'll use any existing page that is less than 50% used, to improve space efficiency. If we have less than one page amount of remaining bytes: we'll keep using existing pages so long as there is enough space for at least 50% of the remaining bytes. This way, there are at most log(n) allocations while reducing existing free space waste.
      if free < min(self.spage_size, overflow_rem_len) / 2 {
        break;
      };
      // Load the existing page with its existing entries.
      let page = self.pages.get(&page_dev_offset).unwrap();
      let mut page_data = PageData::<K, V>::new(self.spage_size, page_dev_offset);
      for &k in page.entries.iter() {
        // Can't be in `keys_to_write`, as otherwise we should've already evicted the page.
        assert!(keys_to_write.contains(&k));
        page_data
          .maybe_write(k, getter(k).unwrap().deref())
          .unwrap();
      }
      // Insert as many overflow entries as possible.
      while let Some(o) = overflow.pop_front() {
        if page_data.maybe_write_raw(&o).is_none() {
          overflow.push_front(o);
          break;
        };
        overflow_rem_len -= u64!(o.len());
      }
      overflow_page_replacements.push((page_data, free));
    }
    for (page_data, original_free_space) in overflow_page_replacements.iter() {
      assert!(self
        .by_free_space
        .remove(&(*original_free_space, page_data.dev_offset)));
    }

    // For the remaining overflow entries, allocate new pages.
    let mut new_overflow_pages = VecDeque::new();
    while let Some(o) = overflow.pop_front() {
      let page_dev_offset = alloc.allocate(self.spage_size);
      let mut page_data = PageData::<K, V>::new(self.spage_size, page_dev_offset);
      page_data.maybe_write_raw(&o).unwrap();
      while let Some(o) = overflow.pop_front() {
        if page_data.maybe_write_raw(&o).is_none() {
          overflow.push_front(o);
          break;
        };
      }
      new_overflow_pages.push_back(page_data);
    }

    // To summarise the situation at this point:
    // - We have built the replacement data for the dirty pages in `dirty_page_replacements`. We must write back to the same page (device offset), as otherwise we'll break the linked list; consider that dirty pages may not be adjacent.
    // - We have built the replacement data for existing non-dirty pages in `overflow_page_replacements`. These must go to their specific current page device offset, and contain their existing data plus some new data.
    // - We have allocated some new pages and built their data in `new_overflow_pages`. We need to append these into the linked list of pages somewhere (anywhere is fine).
    // - We have updated `self.{by_free_space,dirty}` but not `self.{key_to_page,pages}`.
    // - Only keys in `keys_to_write` may have updated pages; `overflow` only contains a subset of keys in `keys_to_write`. All other keys (including existing ones in `overflow_page_replacements`) are unchanged.

    // We will append new overflow pages after the first dirty page. For all other dirty pages, and the existing non-dirty overflow pages, retrieve their existing `next_dev_offset` value from `self.pages`, write it in the `PageData`, and then write back to the same page device offset.
    let (mut first_dirty_page, other_dirty_pages) = (
      dirty_page_replacements.pop().unwrap(),
      dirty_page_replacements,
    );
    let first_dirty_page_next_dev_offset = self.pages[&first_dirty_page.dev_offset]
      .next_dev_offset
      .unwrap_or(0);
    if new_overflow_pages.is_empty() {
      first_dirty_page.write_next_dev_offset(first_dirty_page_next_dev_offset);
      txn.record(first_dirty_page.dev_offset, first_dirty_page.raw);
    } else {
      let first_new_overflow_page_dev_offset = new_overflow_pages.front().unwrap().dev_offset;
      while let Some(mut p) = new_overflow_pages.pop_front() {
        let next_dev_offset = new_overflow_pages
          .front()
          .map(|p| p.dev_offset)
          .unwrap_or(first_dirty_page_next_dev_offset);
        p.write_next_dev_offset(next_dev_offset);
        txn.record(p.dev_offset, p.raw);
      }
      first_dirty_page.write_next_dev_offset(first_new_overflow_page_dev_offset);
      txn.record(first_dirty_page.dev_offset, first_dirty_page.raw);
    };
    for mut p in other_dirty_pages {
      p.write_next_dev_offset(self.pages[&p.dev_offset].next_dev_offset.unwrap_or(0));
      txn.record(p.dev_offset, p.raw);
    }
    for (mut p, _) in overflow_page_replacements {
      p.write_next_dev_offset(self.pages[&p.dev_offset].next_dev_offset.unwrap_or(0));
      txn.record(p.dev_offset, p.raw);
    }

    // Update self.
    todo!();
  }
}
