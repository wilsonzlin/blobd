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
- Occasionally recompacting by scanning the microtile sequentually and moving data towards byte address zero such that there are no gaps (e.g. deletions).

Structure
---------

u24[16777216] microtile_free_space_in_bytes_or_16777215_if_free_tile_or_16777214_if_used_tile

**/

#define FREELIST_RESERVED_SPACE (3 * 16777216)

typedef struct {
  uint64_t dev_offset;
  pthread_rwlock_t rwlock;
  // Bits are set if they're free.
  uint64_t tile_bitmap_1;
  uint64_t tile_bitmap_2[64];
  uint64_t tile_bitmap_3[64][64];
  uint64_t tile_bitmap_4[64][64][64];
  // Storing free amount (instead of used) allows us to initialise these in-memory structures to empty and not available by default, which is faster and more importantly, ensures that all elements including out-of-bound ones that map to invalid tiles are unavailable by default.
  // For each element, bits [31:8] represent the maximum free amount of all elements in the next layer, and [7:0] the corresponding index of the maximum in the next layer (although only 4 bits are used as there are only 16 elements per vector).
  vec_512i_u32_t microtile_free_map_1;
  vec_512i_u32_t microtile_free_map_2[16];
  vec_512i_u32_t microtile_free_map_3[16][16];
  vec_512i_u32_t microtile_free_map_4[16][16][16];
  vec_512i_u32_t microtile_free_map_5[16][16][16][16];
  vec_512i_u32_t microtile_free_map_6[16][16][16][16][16];
  uint64_t dirty_tiles_bitmap_1;
  // Use one-dimensional arrays instead of multidimensional so we can quickly clear using memset.
  uint64_t dirty_tiles_bitmap_2[64];
  uint64_t dirty_tiles_bitmap_3[64 * 64];
  uint64_t dirty_tiles_bitmap_4[64 * 64 * 64];
} freelist_t;

typedef struct {
  uint32_t microtile;
  uint32_t microtile_offset;
} freelist_consumed_microtile_t;

freelist_t* freelist_create_from_disk_state(device_t* dev, uint64_t dev_offset);

void freelist_replenish_tiles_of_inode(freelist_t* fl, cursor_t* inode_cur);

void freelist_consume_tiles(freelist_t* fl, uint64_t tiles_needed, cursor_t* out);

uint32_t freelist_consume_one_tile(freelist_t* fl);

freelist_consumed_microtile_t freelist_consume_microtiles(freelist_t* fl, uint64_t bytes_needed);
