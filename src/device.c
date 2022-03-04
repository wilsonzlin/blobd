#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "bucket.h"
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "freelist.h"
#include "journal.h"
#include "log.h"
#include "stream.h"
#include "tile.h"
#include "util.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("device");

device_t* device_create(void* mmap, uint64_t size) {
  device_t* dev = malloc(sizeof(device_t));
  dev->mmap = mmap;
  dev->size = size;
  dev->tile_count = uint_divide_ceil(size, TILE_SIZE);
  return dev;
}

// Repeat the `entry_size` bytes at `dest` `entries - 1` times.
static inline void memcpy_repeat(void* dest, uint64_t entry_size, uint64_t entries) {
  for (
    uint64_t copies = 1, remaining = entries - 1;
    remaining;
  ) {
    uint64_t n = min(remaining, copies);
    memcpy(dest + (copies * entry_size), dest, n * entry_size);
    remaining -= n;
    copies += n;
  }
}

void device_format(device_t* dev, uint8_t bucket_count_log2) {
  uint64_t jnl_size = JOURNAL_RESERVED_SPACE;
  uint64_t stream_size = STREAM_RESERVED_SPACE;
  uint64_t freelist_size = FREELIST_RESERVED_SPACE;
  uint64_t bkt_cnt = 1llu << bucket_count_log2;
  uint64_t bkts_size = BUCKETS_RESERVED_SPACE(bkt_cnt);
  uint64_t total_size = jnl_size + stream_size + freelist_size + bkts_size;

  // Write empty journal.
  ts_log(INFO, "Writing journal");
  memset(dev->mmap, 0, jnl_size);
  uint64_t jnl_hash = XXH3_64bits(dev->mmap + 8, 4);
  write_u64(dev->mmap, jnl_hash);

  // Write initial stream state.
  ts_log(INFO, "Writing initial stream state");
  cursor_t* stream_cur = dev->mmap + jnl_size;
  write_u64(stream_cur, 1);
  write_u64(stream_cur + 8, 1);
  memset(stream_cur + 16, 0, STREAM_EVENTS_BUF_LEN * (1 + 5 + 8));

  // Calculate freelist usage by metadata.
  cursor_t* freelist_cur = dev->mmap + jnl_size + stream_size;
  uint64_t used_tiles = uint_divide_ceil(total_size, TILE_SIZE);

  // Write fully-used freelist bitmaps.
  ts_log(INFO, "Writing used freelist");
  write_u24(freelist_cur, 16777214);
  memcpy_repeat(freelist_cur, 3, used_tiles);

  // Write unused freelist bitmaps.
  ts_log(INFO, "Writing empty freelist");
  freelist_cur += used_tiles * 3;
  write_u24(freelist_cur, 16777215);
  memcpy_repeat(freelist_cur, 3, 16777216 - used_tiles);

  // Write empty buckets.
  ts_log(INFO, "Writing buckets");
  cursor_t* bkt_cur = dev->mmap + jnl_size + stream_size + freelist_size;
  produce_u8(&bkt_cur, bucket_count_log2);
  memset(bkt_cur, 0, bkt_cnt * 6);

  ts_log(INFO, "Synchronising");
  if (-1 == msync(dev->mmap, dev->size, MS_SYNC)) {
    perror("Failed to sync mmap to device");
    exit(EXIT_INTERNAL);
  }

  ts_log(INFO, "All done!");
}
