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
#include "../ext/klib/khash.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("bucket");

KHASH_SET_INIT_INT64(buckets_pending_delete_or_commit);

struct buckets_s {
  uint64_t dev_offset_pointers;
  bucket_t* buckets;
  uint64_t** dirty_pointers;
  // Must be at least 1.
  uint8_t dirty_pointers_layer_count;
  pthread_rwlock_t dirty_pointers_rwlock;
  uint64_t count;
  uint64_t key_mask;
  kh_buckets_pending_delete_or_commit_t* pending_delete_or_commit;
  pthread_mutex_t pending_delete_or_commit_lock;
};

buckets_t* buckets_create_from_disk_state(
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
  for (uint8_t i = 0, l = 1; i < dirty_layer_count; i++, l *= 64) {
    bkts->dirty_pointers[i] = calloc(l, sizeof(uint64_t));
  }
  if (pthread_rwlock_init(&bkts->dirty_pointers_rwlock, NULL)) {
    perror("Failed to initialise buckets lock");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "Loading %zu buckets", bkts->count);
  cursor_t* cur = dev->mmap + bkts->dev_offset_pointers;
  bkts->buckets = malloc(sizeof(bucket_t) * bkts->count);
  for (uint64_t bkt_id = 0; bkt_id < bkts->count; bkt_id++) {
    bucket_t* bkt = buckets_get_bucket(bkts, bkt_id);
    // TODO Check tile address is less than tile_count.
    bkt->tile = consume_u24(&cur);
    bkt->tile_offset = consume_u24(&cur);
    if (pthread_rwlock_init(&bkt->lock, NULL)) {
      perror("Failed to initialise bucket lock");
      exit(EXIT_INTERNAL);
    }
    bkt->pending_flush_changes = 0;
  }

  bkts->pending_delete_or_commit = kh_init_buckets_pending_delete_or_commit();
  if (pthread_mutex_init(&bkts->pending_delete_or_commit_lock, NULL)) {
    perror("Failed to initialise buckets pending delete or commit lock");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "Loaded buckets");

  return bkts;
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

void buckets_mark_bucket_as_dirty_without_locking(buckets_t* bkts, uint64_t bkt_id) {
  for (
    uint64_t o = bkt_id, i = bkts->dirty_pointers_layer_count;
    i > 0;
    o /= 64, i--
  ) {
    bkts->dirty_pointers[i - 1][o / 64] |= (1llu << (o % 64));
  }
}

void buckets_mark_bucket_as_dirty(buckets_t* bkts, uint64_t bkt_id) {
  if (pthread_rwlock_wrlock(&bkts->dirty_pointers_rwlock)) {
    perror("Failed to acquire write lock on buckets");
    exit(EXIT_INTERNAL);
  }
  buckets_mark_bucket_as_dirty_without_locking(bkts, bkt_id);
  if (pthread_rwlock_unlock(&bkts->dirty_pointers_rwlock)) {
    perror("Failed to release write lock on buckets");
    exit(EXIT_INTERNAL);
  }
}

uint32_t buckets_pending_delete_or_commit_iterator_end(buckets_t* bkts) {
  return kh_end(bkts->pending_delete_or_commit);
}

uint64_t buckets_pending_delete_or_commit_iterator_get(buckets_t* bkts, uint32_t it) {
  if (!kh_exist(bkts->pending_delete_or_commit, it)) {
    return 0;
  }
  return kh_key(bkts->pending_delete_or_commit, it);
}

void buckets_pending_delete_or_commit_lock(buckets_t* bkts) {
  if (pthread_mutex_lock(&bkts->pending_delete_or_commit_lock)) {
    perror("Failed to acquire lock on buckets pending delete or commit");
    exit(EXIT_INTERNAL);
  }
}

void buckets_pending_delete_or_commit_unlock(buckets_t* bkts) {
  if (pthread_mutex_unlock(&bkts->pending_delete_or_commit_lock)) {
    perror("Failed to release lock on buckets pending delete or commit");
    exit(EXIT_INTERNAL);
  }
}

void buckets_mark_bucket_as_pending_delete_or_commit(buckets_t* bkts, uint64_t bkt_id) {
  buckets_pending_delete_or_commit_lock(bkts);
  int kh_ret;
  kh_put_buckets_pending_delete_or_commit(bkts->pending_delete_or_commit, bkt_id, &kh_ret);
  if (kh_ret == -1) {
    fprintf(stderr, "Failed to add to buckets pending delete or commit\n");
    exit(EXIT_INTERNAL);
  }
  buckets_pending_delete_or_commit_unlock(bkts);
}

void buckets_clear_pending_delete_or_commit(buckets_t* bkts) {
  kh_clear_buckets_pending_delete_or_commit(bkts->pending_delete_or_commit);
}

uint64_t buckets_get_device_offset_of_bucket(buckets_t* bkts, uint64_t bkt_id) {
  return bkts->dev_offset_pointers + bkt_id * 6;
}
