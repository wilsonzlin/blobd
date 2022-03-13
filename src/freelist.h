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

#define FREELIST_OFFSETOF_TILE(tile_no) (3 * (tile_no))

#define FREELIST_TILE_CAP 16777216

#define FREELIST_RESERVED_SPACE FREELIST_OFFSETOF_TILE(FREELIST_TILE_CAP)

typedef struct freelist_s freelist_t;

freelist_t* freelist_create_from_disk_state(device_t* dev, uint64_t dev_offset);

void freelist_lock(freelist_t* fl);

void freelist_unlock(freelist_t* fl);

void freelist_replenish_tiles_of_inode(freelist_t* fl, cursor_t* inode_cur);

void freelist_consume_tiles(freelist_t* fl, uint64_t tiles_needed, cursor_t* out);

uint32_t freelist_consume_one_tile(freelist_t* fl);

uint64_t freelist_consume_microtile_space(freelist_t* fl, uint32_t bytes_needed);
