#pragma once

#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include "cursor.h"
#include "vec.h"

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
- Occasionally recompacting by scanning the microtile sequentually and moving data towards byte address zero such that there are no gaps (e.g. deletions). The info in inodes will need to be updated simultaneously.

Structure
---------

{
  u8 tile_bitmap
  u24[8] microtile_usage_in_bytes_minus_one_or_zero_if_not_microtile
  u64 xxhash
}[2097152] eight_tiles

**/

typedef struct {
  size_t dev_offset;
  pthread_rwlock_t rwlock;
  uint64_t tile_bitmap_1;
  uint64_t tile_bitmap_2[64];
  uint64_t tile_bitmap_3[64][64];
  uint64_t tile_bitmap_4[64][64][64];
  // Note that we store free amount, which is opposite to device representation. This allows us to initialise these in-memory structures to empty and not available by default, which is faster and more importantly, ensures that all elements including out-of-bound ones that map to invalid tiles are unavailable by default.
  vec_512i_u32_t microtile_free_map_1;
  vec_512i_u32_t microtile_free_map_2[16];
  vec_512i_u32_t microtile_free_map_3[16][16];
  vec_512i_u32_t microtile_free_map_4[16][16][16];
  vec_512i_u32_t microtile_free_map_5[16][16][16][16];
  vec_512i_u32_t microtile_free_map_6[16][16][16][16][16];
  // Since we checksum both tile and microtile metadata, we only need one dirty bitmap for both tile and microtile changes.
  uint8_t dirty_eight_tiles_bitmap_1;
  // Use one-dimensional arrays instead of multidimensional so we can quickly clear using memset.
  uint64_t dirty_eight_tiles_bitmap_2[8];
  uint64_t dirty_eight_tiles_bitmap_3[64 * 8];
  uint64_t dirty_eight_tiles_bitmap_4[64 * 64 * 8];
} freelist_t;

typedef struct {
  uint32_t microtile;
  uint32_t microtile_offset;
} freelist_consumed_microtile_t;

freelist_t* freelist_create_from_disk_state(device_t* dev, size_t dev_offset);

void freelist_consume_tiles(freelist_t* fl, size_t tiles_needed, cursor_t** out);

uint32_t freelist_consume_one_tile(freelist_t* fl);

freelist_consumed_microtile_t freelist_consume_microtiles(freelist_t* fl, size_t bytes_needed);
