#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "journal.h"
#include "log.h"
#include "tile.h"
#include "util.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("device");

device_t* device_create(void* mmap, size_t size) {
  device_t* dev = malloc(sizeof(device_t));
  dev->mmap = mmap;
  dev->size = size;
  dev->tile_count = uint_divide_ceil(size, TILE_SIZE);
  return dev;
}

// Repeat the `entry_size` bytes at `dest` `entries - 1` times.
static inline void memcpy_repeat(void* dest, size_t entry_size, size_t entries) {
  for (
    size_t copies = 1, remaining = entries - 1;
    remaining;
  ) {
    size_t n = min(remaining, copies);
    memcpy(dest + (copies * entry_size), dest, n * entry_size);
    remaining -= n;
    copies += n;
  }
}

void device_format(device_t* dev, uint8_t bucket_count_log2) {
  size_t jnl_size = JOURNAL_RESERVED_SPACE;
  size_t fl_entry_size = 1 + 3 * 8 + 8;
  size_t freelist_size = fl_entry_size * 2097152;
  size_t bkt_entry_size = 6 * 16 + 8;
  size_t bkt_16s = (1llu << bucket_count_log2) / 16;
  size_t bkts_size = bkt_16s * bkt_entry_size;
  size_t total_size = jnl_size + freelist_size + bkts_size;

  // Write empty journal.
  ts_log(DEBUG, "Writing journal");
  memset(dev->mmap, 0, jnl_size);
  uint64_t jnl_hash = XXH3_64bits(dev->mmap + 8, 4);
  write_u64(dev->mmap, jnl_hash);

  // Calculate freelist usage by metadata.
  cursor_t* freelist_cur = dev->mmap + jnl_size;
  size_t used_tiles = uint_divide_ceil(total_size, TILE_SIZE);
  size_t used_tile_8s_full = used_tiles / 8;
  uint8_t used_tile_8s_last_bitmap = ~((1 << (used_tiles % 8)) - 1);

  // Write fully-used freelist bitmaps.
  ts_log(DEBUG, "Writing used freelist");
  memset(freelist_cur, 0, fl_entry_size - 8);
  uint64_t fl_full_hash = XXH3_64bits(freelist_cur, fl_entry_size - 8);
  write_u64(freelist_cur + fl_entry_size - 8, fl_full_hash);
  memcpy_repeat(freelist_cur, fl_entry_size, used_tile_8s_full);

  // Write partially-used freelist bitmap.
  freelist_cur += fl_entry_size * used_tile_8s_full;
  memset(freelist_cur, 0, fl_entry_size - 8);
  freelist_cur[0] = used_tile_8s_last_bitmap;
  uint64_t fl_last_hash = XXH3_64bits(freelist_cur, fl_entry_size - 8);
  write_u64(freelist_cur + fl_entry_size - 8, fl_last_hash);

  // Write unused freelist bitmaps.
  ts_log(DEBUG, "Writing empty freelist");
  freelist_cur += fl_entry_size * 1;
  memset(freelist_cur, 0, fl_entry_size - 8);
  freelist_cur[0] = 0xFF;
  uint64_t fl_empty_hash = XXH3_64bits(freelist_cur, fl_entry_size - 8);
  write_u64(freelist_cur + fl_entry_size - 8, fl_empty_hash);
  memcpy_repeat(freelist_cur, fl_entry_size, 2097152 - 1 - used_tile_8s_full);

  // Write empty buckets.
  ts_log(DEBUG, "Writing buckets");
  cursor_t* bkt_cur = dev->mmap + jnl_size + freelist_size;
  produce_u8(&bkt_cur, bucket_count_log2);
  memset(bkt_cur, 0, bkt_entry_size - 8);
  uint64_t bkt_hash = XXH3_64bits(bkt_cur, bkt_entry_size - 8);
  write_u64(bkt_cur + bkt_entry_size - 8, bkt_hash);
  memcpy_repeat(bkt_cur, bkt_entry_size, bkt_16s);

  ts_log(DEBUG, "Synchronising");
  if (-1 == msync(dev->mmap, dev->size, MS_SYNC)) {
    perror("Failed to sync mmap to device");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "All done!");
}
