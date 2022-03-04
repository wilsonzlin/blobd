#pragma once

#include <stdint.h>
#include "device.h"
#include "../ext/klib/khash.h"

/**

BUCKET
======

Since we hash keys, we expect keys to have no correlation to their buckets. As such, we will likely jump between random buckets on each key lookup, and if they are not loaded into memory, we'll end up continuously paging in and out of disk, ruining any performance gain of using a hash map structure.

The limit on the amount of buckets is somewhat arbitrary, but provides reasonable constraints that we can target and optimise for.

Because we allow deletion of objects and aim for immediate freeing of space, we must use locks on each bucket, as deleting requires detaching the inode, which means modifying non-atomic heap data. If we modified the memory anyway, other readers/writers may jump to arbitrary positions and possibly leak sensitive data or crash. Also, simultaneous creations/deletions on the same bucket would cause race conditions. Creating an object also requires a lock; it's possible to do an atomic CAS on the in-memory bucket head, but then the bucket would point to an inode with an uninitialised or zero "next" link.

Structure
---------

u8 count_log2_between_12_and_40_inclusive
{
  u24 tile_or_zero
  u24 tile_offset
}[] heads

**/

#define BUCKETS_RESERVED_SPACE(bkt_cnt) (1 + (bkt_cnt) * 6)

typedef struct {
  pthread_rwlock_t lock;
  uint32_t tile;
  uint32_t tile_offset;
  // For use by flush.c only.
  uint64_t pending_flush_changes;
} bucket_t;

typedef struct buckets_s buckets_t;

buckets_t* buckets_create_from_disk_state(device_t* dev, uint64_t dev_offset);

uint64_t buckets_get_count(buckets_t* bkts);

uint64_t buckets_get_bucket_id_for_key(buckets_t* bkts, uint8_t* key, uint8_t key_len);

bucket_t* buckets_get_bucket(buckets_t* bkts, uint64_t bkt_id);

uint8_t buckets_get_dirty_bitmap_layer_count(buckets_t* bkts);

uint64_t* buckets_get_dirty_bitmap_layer(buckets_t* bkts, uint8_t layer);

void buckets_mark_bucket_as_dirty_without_locking(buckets_t* bkts, uint64_t bkt_id);

void buckets_mark_bucket_as_dirty(buckets_t* bkts, uint64_t bkt_id);

uint32_t buckets_pending_delete_or_commit_iterator_end(buckets_t* bkts);

// Returns 0 if no value is at this iterator position.
uint64_t buckets_pending_delete_or_commit_iterator_get(buckets_t* bkts, uint32_t it);

void buckets_pending_delete_or_commit_lock(buckets_t* bkts);

void buckets_pending_delete_or_commit_unlock(buckets_t* bkts);

// Lock will be acquired.
void buckets_mark_bucket_as_pending_delete_or_commit(buckets_t* bkts, uint64_t bkt_id);

// Lock must already be acquired.
void buckets_clear_pending_delete_or_commit(buckets_t* bkts);

uint64_t buckets_get_device_offset_of_bucket(buckets_t* bkts, uint64_t bkt_id);
