use std::{cmp::Ordering, collections::BinaryHeap};

use off64::Off64;
use roaring::RoaringBitmap;
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

u24[16777216] tile_used_space_in_bytes

**/

#[allow(non_snake_case)]
pub fn FREELIST_OFFSETOF_TILE(tile_no: u32) -> u64 { 3 * u64::from(tile_no) }

pub const FREELIST_TILE_CAP: u32 =  16777216;

#[allow(non_snake_case)]
pub fn FREELIST_RESERVED_SPACE() -> u64 { FREELIST_OFFSETOF_TILE(FREELIST_TILE_CAP) }

#[repr(u8)]
pub enum FragmentType {
  Inode,
  TailData,
}

#[derive(PartialEq, Eq)]
struct TileSpace {
  tile_no: u32,
  free_bytes: u32,
}

impl PartialOrd for TileSpace {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.free_bytes.partial_cmp(&other.free_bytes)
  }
}

impl Ord for TileSpace {
  fn cmp(&self, other: &Self) -> Ordering {
    self.partial_cmp(other).unwrap()
  }
}

pub struct FreeList {
  dev_offset: u64,
  solid_tiles: RoaringBitmap,
  fragment_tiles: BinaryHeap<TileSpace>,
}

impl FreeList {
  pub fn load_from_device(&mut self, device: SeekableAsyncFile, tile_count: u32) {
    for tile_no in 0..tile_count {
      let used_bytes = device.read_at_sync(FREELIST_OFFSETOF_TILE(tile_no), 3).read_u24_be_at(0);
      if used_bytes == 0 {
        self.solid_tiles.insert(used_bytes);
      } else {
        self.fragment_tiles.push(TileSpace { tile_no, free_bytes: TILE_SIZE - used_bytes });
      };
    };
  }

  // `bytes_needed` must include the leading FragmentType tag.
  // Returns the device offset of the fragment, and the updated fragmented tile usage amount. Make sure to write the FragmentType tag.
  pub fn allocate_fragment(&mut self, bytes_needed: u32) -> (u64, u32) {
    // Trying to find the fragment tile with the closest free_bytes to bytes_needed is an unnecessary optimisation, since it's intractable to know if that's actually optimal or not without the ability to predict the future.
    let (tile_no, cur_free_bytes) = if self.fragment_tiles.peek().filter(|t| t.free_bytes >= bytes_needed).is_some() {
      let t = self.fragment_tiles.pop().unwrap();
      (t.tile_no, t.free_bytes)
    } else {
      // We've run out of fragment tiles, so allocate a new one.
      let Some(tile_no) = self.solid_tiles.min() else {
        panic!("out of storage space");
      };
      self.solid_tiles.remove(tile_no);
      (tile_no, TILE_SIZE)
    };
    let dev_offset = FREELIST_OFFSETOF_TILE(tile_no) +  u64::from(TILE_SIZE - cur_free_bytes);
    let new_free_bytes = cur_free_bytes - bytes_needed;
    if new_free_bytes > 0 {
      self.fragment_tiles.push(TileSpace { tile_no, free_bytes: new_free_bytes });
    };
    (dev_offset, TILE_SIZE - new_free_bytes)
  }

  pub fn allocate_tiles(&mut self, tiles_needed: u16) -> Vec<u32> {
    if self.solid_tiles.len() < u64::from(tiles_needed) {
      panic!("out of storage space");
    };
    let mut tiles = vec![];
    for _ in 0..tiles_needed {
      let tile_no = self.solid_tiles.min().unwrap();
      self.solid_tiles.remove(tile_no);
      tiles.push(tile_no);
    };
    tiles
  }

  pub fn release_tiles(&mut self, tiles: &[u32]) {
    for tile_no in tiles {
      self.solid_tiles.insert(*tile_no);
    };
  }
}
