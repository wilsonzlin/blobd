#define _GNU_SOURCE

#include <inttypes.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "bucket.h"
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "inode.h"
#include "log.h"
#include "tile.h"
#include "util.h"
#include "../ext/klib/khash.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("bucket");

struct buckets_s {
  uint64_t dev_offset_pointers;
  bucket_t* buckets;
  uint64_t** dirty_pointers;
  // Must be at least 1.
  uint8_t dirty_pointers_layer_count;
  uint64_t count;
  uint64_t key_mask;
};

buckets_t* buckets_create_from_disk_state(
  inodes_state_t* inodes_state,
  device_t* dev,
  uint64_t dev_offset
) {
  buckets_t* bkts = malloc(sizeof(buckets_t));
  uint8_t count_log2 = dev->mmap[dev_offset];
  bkts->count = 1llu << count_log2;
  bkts->key_mask = bkts->count - 1;
  bkts->dev_offset_pointers = dev_offset + 1;

  uint8_t dirty_layer_count = 1;
  for (uint64_t v = 64; v < bkts->count; v *= 64) {
    dirty_layer_count++;
  }
  bkts->dirty_pointers_layer_count = dirty_layer_count;
  bkts->dirty_pointers = malloc(sizeof(uint64_t*) * dirty_layer_count);
  // Use uint64_t for `l`.
  for (uint64_t i = 0, l = 1; i < dirty_layer_count; i++, l *= 64) {
    bkts->dirty_pointers[i] = calloc(l, sizeof(uint64_t));
  }

  ts_log(INFO, "Loading %zu buckets", bkts->count);
  cursor_t* cur = dev->mmap + bkts->dev_offset_pointers;
  bkts->buckets = malloc(sizeof(bucket_t) * bkts->count);
  for (uint64_t bkt_id = 0; bkt_id < bkts->count; bkt_id++) {
    bucket_t* bkt = buckets_get_bucket(bkts, bkt_id);
    atomic_init(&bkt->head, NULL);
    // TODO Check tile address is less than tile_count.
    uint32_t tile = consume_u24(&cur);
    uint32_t tile_offset = consume_u24(&cur);
    inode_t* prev = NULL;
    while (tile) {
      cursor_t* ino_cur = dev->mmap + (TILE_SIZE * tile) + tile_offset;
      // TODO Check tile address is less than tile_count.
      uint32_t next_tile = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE);
      uint32_t next_tile_offset = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET);
      ino_state_t ino_state = cur[INO_OFFSETOF_STATE];
      inode_t* bkt_ino = inode_create_thread_unsafe(inodes_state, NULL, ino_state, tile, tile_offset);
      if (prev != NULL) atomic_store_explicit(&prev->next, bkt_ino, memory_order_relaxed);
      if (atomic_load_explicit(&bkt->head, memory_order_relaxed) == NULL) atomic_store_explicit(&bkt->head, bkt_ino, memory_order_relaxed);
    }
  }

  ts_log(INFO, "Loaded buckets");

  return bkts;
}

uint64_t buckets_get_count(buckets_t* bkts) {
  return bkts->count;
}

uint64_t buckets_get_bucket_id_for_key(buckets_t* bkts, uint8_t* key, uint8_t key_len) {
  return XXH3_64bits(key, key_len) & bkts->key_mask;
}

bucket_t* buckets_get_bucket(buckets_t* bkts, uint64_t bkt_id) {
  return bkts->buckets + bkt_id;
}

uint8_t buckets_get_dirty_bitmap_layer_count(buckets_t* bkts) {
  return bkts->dirty_pointers_layer_count;
}

uint64_t* buckets_get_dirty_bitmap_layer(buckets_t* bkts, uint8_t layer) {
  return bkts->dirty_pointers[layer];
}

void buckets_mark_bucket_as_dirty(buckets_t* bkts, uint64_t bkt_id) {
  for (
    uint64_t o = bkt_id, i = bkts->dirty_pointers_layer_count;
    i > 0;
    o /= 64, i--
  ) {
    bkts->dirty_pointers[i - 1][o / 64] |= (1llu << (o % 64));
  }
}

uint64_t buckets_get_device_offset_of_bucket(buckets_t* bkts, uint64_t bkt_id) {
  return bkts->dev_offset_pointers + bkt_id * 6;
}
