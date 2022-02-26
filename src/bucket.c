#define _GNU_SOURCE

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include "bucket.h"
#include "cursor.h"
#include "device.h"
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
  // TODO Ensure this does not overflow.
  size_t bkt_cnt = 1 << bkts->count_log2;
  // This will always divide perfectly because count_log2 must be greater than or equal to 12.
  size_t sixteens = bkt_cnt / 16;

  size_t dirty_layer_count = 1;
  for (size_t v = 64; v < sixteens; v *= 64) {
    dirty_layer_count++;
  }
  bkts->dirty_sixteen_pointers_layer_count = dirty_layer_count;
  bkts->dirty_sixteen_pointers = malloc(sizeof(uint64_t*) * dirty_layer_count);
  for (size_t i = 0, l = 1; i < dirty_layer_count; i++, l *= 64) {
    bkts->dirty_sixteen_pointers[i] = aligned_alloc(sizeof(uint64_t), sizeof(uint64_t) * l);
  }

  cursor_t* cur = dev->mmap + bkts->dev_offset_pointers;
  bkts->bucket_pointers = malloc(sizeof(atomic_uint_least64_t) * bkt_cnt);
  for (size_t i = 0; i < sixteens; i++) {
    uint64_t checksum_actual = XXH3_64bits(cur, 6 * 16);
    for (size_t j = 0; j < 16; j++) {
      size_t idx = i * 16 + j;
      // TODO Check tile address is less than tile_count.
      bkts->bucket_pointers[idx] = (consume_u24(&cur) << 24) | consume_u24(&cur);
    }
    uint64_t checksum_recorded = consume_u64(&cur);
    if (checksum_recorded != checksum_actual) {
      CORRUPT("invalid bucket data hash at 16-bucket group %zu, recorded hash is %"PRIx64" but recorded data hashes to %"PRIx64, i, checksum_recorded, checksum_actual);
    }
  }

  return bkts;
}
