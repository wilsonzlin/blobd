#define _GNU_SOURCE

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "bucket.h"
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "log.h"
#include "util.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("bucket");

buckets_t* buckets_create_from_disk_state(
  device_t* dev,
  size_t dev_offset
) {
  buckets_t* bkts = malloc(sizeof(buckets_t));
  bkts->count_log2 = dev->mmap[dev_offset];
  bkts->dev_offset_pointers = dev_offset + 1;
  uint64_t bkt_cnt = 1llu << bkts->count_log2;

  size_t dirty_layer_count = 1;
  for (uint64_t v = 64; v < bkt_cnt; v *= 64) {
    dirty_layer_count++;
  }
  bkts->dirty_pointers_layer_count = dirty_layer_count;
  bkts->dirty_pointers = malloc(sizeof(uint64_t*) * dirty_layer_count);
  for (size_t i = 0, l = 1; i < dirty_layer_count; i++, l *= 64) {
    bkts->dirty_pointers[i] = calloc(l, sizeof(uint64_t));
  }
  if (pthread_rwlock_init(&bkts->dirty_pointers_rwlock, NULL)) {
    perror("Failed to initialise buckets lock");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "Loading %zu buckets", bkt_cnt);
  cursor_t* cur = dev->mmap + bkts->dev_offset_pointers;
  bkts->buckets = malloc(sizeof(bucket_t) * bkt_cnt);
  for (uint64_t bkt_id = 0; bkt_id < bkt_cnt; bkt_id++) {
    // TODO Check tile address is less than tile_count.
    bkts->buckets[bkt_id].microtile = consume_u24(&cur);
    bkts->buckets[bkt_id].microtile_byte_offset = consume_u24(&cur);
    if (pthread_rwlock_init(&bkts->buckets[bkt_id].lock, NULL)) {
      perror("Failed to initialise bucket lock");
      exit(EXIT_INTERNAL);
    }
  }

  ts_log(DEBUG, "Loaded buckets");

  return bkts;
}
