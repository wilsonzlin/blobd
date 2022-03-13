#include <inttypes.h>
#include <pthread.h>
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

uint64_t buckets_read_count(device_t* dev, uint64_t dev_offset) {
  uint8_t count_log2 = dev->mmap[dev_offset];
  return 1llu << count_log2;
}

buckets_t* buckets_create_from_disk_state(
  device_t* dev,
  uint64_t dev_offset
) {
  buckets_t* bkts = malloc(sizeof(buckets_t));
  bkts->count = buckets_read_count(dev, dev_offset);
  bkts->dev_offset = dev_offset;
  bkts->key_mask = bkts->count - 1;

  ts_log(INFO, "Loading %zu buckets", bkts->count);
  bkts->bucket_locks = malloc(sizeof(pthread_rwlock_t) * bkts->count);
  for (uint64_t bkt_id = 0; bkt_id < bkts->count; bkt_id++) {
    ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&bkts->bucket_locks[bkt_id], NULL), "init bucket lock");
  }

  ts_log(INFO, "Loaded buckets");

  return bkts;
}
