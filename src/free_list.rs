use std::{cmp::Ordering, collections::{BinaryHeap, BTreeMap}};

use off64::{Off64Int, usz, create_u24_be};
use roaring::RoaringBitmap;
use rustc_hash::FxHashMap;
use seekable_async_file::SeekableAsyncFile;

use crate::tile::TILE_SIZE;

/**

FREELIST
========

List of free tiles on the device. All possible 2^24 tile metadata is recorded on the device, even if there are not that many tiles, to ensure future resizing of the device doesn't require any data shifting and reallocating.

Actions
-------

Consume: mark one or more tiles as used.

Replenish: mark one or more tiles as free.

Algorithm
---------

Factors to consider:

- The raw data should (already) be cached in memory by the kernel block cache, so we shouldn't need to worry about the latency of each algorithm operation on the data hitting the device.
- We most likely have to read data into memory before working with them as the values on disk are stored unaligned and big endian.
- Since we must flush after every change, and the total raw bytes of the list data is relatively small, it's unlikely to have an impact whether we write/update/flush 1 changed byte or 200, as both are well under the smallest possible write unit (e.g. SSD memory cell).
- The latency of writing to disk and getting an acknowledgement back is much, much higher than an efficient sort over a small amount of data that fits in CPU cache, especially if we're writing to a durable, replicated block volume over the network using iSCSI/TCP.

Reclaiming microtile space
--------------------------

We can reclaim space by:
- Waiting for all objects that use the microtile to become deleted, and then marking the tile as free again (it doesn't have to keep being a microtile).
- Occasionally recompacting by scanning the microtile sequentually and moving data towards byte address zero such that there are no gaps (e.g. deletions).

Structure
---------

{
  u24 tile_used_space_in_bytes
  u24 tile_released_space_in_bytes
}[16777216]

**/

#[allow(non_snake_case)]
pub fn FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no: u32) -> u64 { 6 * u64::from(tile_no) }
#[allow(non_snake_case)]
pub fn FREELIST_OFFSETOF_TILE_RELEASED_SPACE(tile_no: u32) -> u64 { 6 * u64::from(tile_no) + 3 }

pub const FREELIST_TILE_CAP: u32 =  16777216;

#[allow(non_snake_case)]
pub fn FREELIST_SIZE() -> u64 { FREELIST_OFFSETOF_TILE_USED_SPACE(FREELIST_TILE_CAP) }

struct FragmentedTile {
  released_bytes: u32,
  used_bytes: u32,
}

impl FragmentedTile {
  fn free_bytes(&self) -> u32 {
    TILE_SIZE - self.used_bytes
  }

  fn all_released(&self) -> bool {
    self.used_bytes == self.released_bytes
  }
}

struct FragmentedTiles {
  tiles: FxHashMap<u32, FragmentedTile>,
  by_free_space: BTreeMap<u32, RoaringBitmap>,
}

impl FragmentedTiles {
  pub fn new() -> Self {
    Self { tiles: FxHashMap::default(), by_free_space: BTreeMap::new() }
  }

  pub fn add(&mut self, no: u32, t: FragmentedTile) {
    self.by_free_space.entry(TILE_SIZE - t.used_bytes).or_default().insert(no);
    let None = self.tiles.insert(no, t) else {
      unreachable!();
    };
  }

  fn remove_free_space_entry(&mut self, no: u32, free_space: u32) {
    let bitmap = self.by_free_space.get_mut(&free_space).unwrap();
    let removed = bitmap.remove(no);
    assert!(removed);
    // We must remove empty entries so that *_most_free remains O(1).
    if bitmap.is_empty() {
      self.by_free_space.remove(&free_space).unwrap();
    };
  }

  pub fn remove(&mut self, no: u32) -> FragmentedTile {
    let t = self.tiles.remove(&no).unwrap();
    let free_space = TILE_SIZE - t.used_bytes;
    let removed = self.by_free_space.get_mut(&free_space).unwrap().remove(no);
    assert!(removed);
    t
  }

  pub fn most_free(&self) -> Option<u32> {
    if let Some((_, tiles)) = self.by_free_space.last_key_value() {
      let tile_no = tiles.min().unwrap();
      return Some(tile_no);
    };
    None
  }

  pub fn get(&self, no: u32) -> &FragmentedTile {
    self.tiles.get(&no).unwrap()
  }

  pub fn update_released_bytes(&mut self, no: u32, b: u32) {
    self.tiles.get_mut(&no).unwrap().released_bytes = b;
  }

  pub fn update_used_bytes(&mut self, no: u32, new_used: u32) {
    let mut t = self.tiles.get_mut(&no).unwrap();
    let new_free = TILE_SIZE - new_used;
    let old_free = TILE_SIZE - t.used_bytes;
    t.used_bytes = new_used;
    self.remove_free_space_entry(no, old_free);
    let inserted = self.by_free_space.entry(new_free).or_default().insert(no);
    assert!(inserted);
  }
}

pub struct FreeList {
  dev_offset: u64,
  solid_tiles: RoaringBitmap,
  fragmented_tiles: FragmentedTiles,
}

impl FreeList {
  pub fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64, reserved_tile_count: u32, total_tile_count: u32) -> FreeList {
    let mut solid_tiles = RoaringBitmap::new();
    let mut fragmented_tiles = FragmentedTiles::new();
    for tile_no in reserved_tile_count..total_tile_count {
      let dev_offset_used_bytes = FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no);
      let dev_offset_released_bytes = FREELIST_OFFSETOF_TILE_RELEASED_SPACE(tile_no);
      let used_bytes = dev.read_at_sync(dev_offset_used_bytes, 3).read_u24_be_at(0);
      let released_bytes = dev.read_at_sync(dev_offset_released_bytes, 3).read_u24_be_at(0);
      if used_bytes == 0 {
        solid_tiles.insert(used_bytes);
      } else {
        fragmented_tiles.add(tile_no, FragmentedTile { released_bytes, used_bytes });
      };
    };
    FreeList { dev_offset, solid_tiles, fragmented_tiles }
  }

  pub fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at_sync(dev_offset, vec![0u8; usz!(FREELIST_SIZE())]);
  }

  // Returns the device offset of the fragment, and the updated fragmented tile usage amount.
  pub fn allocate_fragment(&mut self, mutation_writes: &mut Vec<(u64, Vec<u8>)>, bytes_needed: u32) -> u64 {
    // Trying to find the fragment tile with the closest free_bytes to bytes_needed is an unnecessary optimisation, since it's intractable to know if that's actually optimal or not without the ability to predict the future.
    let tile_no = match self.fragmented_tiles.most_free().filter(|no| self.fragmented_tiles.get(*no).free_bytes() >= bytes_needed) {
      Some(tile_no) => tile_no,
      None => {
        // We've run out of fragment tiles, so allocate a new one.
        let Some(tile_no) = self.solid_tiles.min() else {
          panic!("out of storage space");
        };
        self.solid_tiles.remove(tile_no);
        self.fragmented_tiles.add(tile_no, FragmentedTile { released_bytes: 0, used_bytes: 0 });
        tile_no
      }
    };
    let cur_free_bytes = self.fragmented_tiles.get(tile_no).free_bytes();
    let dev_offset = self.dev_offset + FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no) +  u64::from(TILE_SIZE - cur_free_bytes);
    let new_free_bytes = cur_free_bytes - bytes_needed;
    self.fragmented_tiles.update_used_bytes(tile_no, TILE_SIZE - new_free_bytes);
    mutation_writes.push((
      self.dev_offset + FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no),
      create_u24_be(TILE_SIZE - new_free_bytes).to_vec(),
    ));
    dev_offset
  }

  pub fn release_fragment(&mut self, mutation_writes: &mut Vec<(u64, Vec<u8>)>, dev_offset: u64, frag_len: u32) -> () {
    let tile_no = u32::try_from(dev_offset / u64::from(TILE_SIZE)).unwrap();
    let new_released_bytes = self.fragmented_tiles.get(tile_no).released_bytes + frag_len;
    self.fragmented_tiles.update_released_bytes(tile_no, new_released_bytes);
    if self.fragmented_tiles.get(tile_no).all_released() {
      self.fragmented_tiles.remove(tile_no);
      self.solid_tiles.insert(tile_no);
      mutation_writes.push((
        self.dev_offset + FREELIST_OFFSETOF_TILE_RELEASED_SPACE(tile_no),
        create_u24_be(0).to_vec(),
      ));
      mutation_writes.push((
        self.dev_offset + FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no),
        create_u24_be(0).to_vec(),
      ));
    } else {
      mutation_writes.push((
        self.dev_offset + FREELIST_OFFSETOF_TILE_RELEASED_SPACE(tile_no),
        create_u24_be(new_released_bytes).to_vec(),
      ));
    };
  }

  pub fn allocate_tiles(&mut self, mutation_writes: &mut Vec<(u64, Vec<u8>)>, tiles_needed: u16) -> Vec<u32> {
    if self.solid_tiles.len() < u64::from(tiles_needed) {
      panic!("out of storage space");
    };
    let mut tiles = vec![];
    for _ in 0..tiles_needed {
      let tile_no = self.solid_tiles.min().unwrap();
      self.solid_tiles.remove(tile_no);
      tiles.push(tile_no);
      mutation_writes.push((
        self.dev_offset + FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no),
        create_u24_be(TILE_SIZE).to_vec(),
      ));
    };
    tiles
  }

  pub fn release_tiles(&mut self, mutation_writes: &mut Vec<(u64, Vec<u8>)>, tiles: &[u32]) {
    for &tile_no in tiles {
      self.solid_tiles.insert(tile_no);
      mutation_writes.push((
        self.dev_offset + FREELIST_OFFSETOF_TILE_USED_SPACE(tile_no),
        create_u24_be(0).to_vec(),
      ));
    };
  }
}
