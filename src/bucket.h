#pragma once

#include <stdatomic.h>
#include <stdint.h>
#include "device.h"
#include "inode.h"
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
  _Atomic(inode_t*) head;
} bucket_t;

typedef struct buckets_s buckets_t;

buckets_t* buckets_create_from_disk_state(inodes_state_t* inodes_state, device_t* dev, uint64_t dev_offset);

uint64_t buckets_get_count(buckets_t* bkts);

uint64_t buckets_get_bucket_id_for_key(buckets_t* bkts, uint8_t* key, uint8_t key_len);

bucket_t* buckets_get_bucket(buckets_t* bkts, uint64_t bkt_id);

uint8_t buckets_get_dirty_bitmap_layer_count(buckets_t* bkts);

uint64_t* buckets_get_dirty_bitmap_layer(buckets_t* bkts, uint8_t layer);

// A bucket is dirty if the head, or any inode's "next" or "state" field value, has changed.
void buckets_mark_bucket_as_dirty(buckets_t* bkts, uint64_t bkt_id);

uint64_t buckets_get_device_offset_of_bucket(buckets_t* bkts, uint64_t bkt_id);
