#pragma once

#include <pthread.h>
#include <stdint.h>
#include "device.h"
#include "inode.h"
#include "../ext/xxHash/xxhash.h"

/**

BUCKET
======

Since we hash keys, we expect keys to have no correlation to their buckets. As such, we will likely jump between random buckets on each key lookup, and if they are not loaded into memory, we'll end up continuously paging in and out of disk, ruining any performance gain of using a hash map structure.

The limit on the amount of buckets is somewhat arbitrary, but provides reasonable constraints that we can target and optimise for.

Because we allow deletion of objects and aim for immediate freeing of space, we must use locks on each bucket, as deleting requires detaching the inode, which means modifying non-atomic heap data. If we modified the memory anyway, other readers/writers may jump to arbitrary positions and possibly leak sensitive data or crash. Also, simultaneous creations/deletions on the same bucket would cause race conditions. Creating an object also requires a lock; it's possible to do an atomic CAS on the in-memory bucket head, but then the bucket would point to an inode with an uninitialised or zero "next" link.

Structure
---------

u8 count_log2_between_12_and_40_inclusive
u48[] dev_offset_or_zero

**/

#define BUCKETS_OFFSETOF_BUCKET(bkt_id) (1 + (bkt_id) * 6)

#define BUCKETS_RESERVED_SPACE(bkt_cnt) BUCKETS_OFFSETOF_BUCKET(bkt_cnt)

#define BUCKET_ID_FOR_KEY(key, key_len, mask) (XXH3_64bits(key, key_len) & (mask))

#define BUCKET_LOCK_READ(bkts, bucket_id) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&(bkts)->buckets[bucket_id].lock), "read-lock bucket")

#define BUCKET_LOCK_WRITE(bkts, bucket_id) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&(bkts)->buckets[bucket_id].lock), "write-lock bucket")

#define BUCKET_UNLOCK(bkts, bucket_id) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&(bkts)->buckets[bucket_id].lock), "unlock bucket")

typedef struct {
  pthread_rwlock_t lock;
} bucket_t;

typedef struct {
  uint64_t count;
  uint64_t dev_offset;
  uint64_t key_mask;
  bucket_t* buckets;
} buckets_t;

uint64_t buckets_read_count(device_t* dev, uint64_t dev_offset);

buckets_t* buckets_create_from_disk_state(device_t* dev, uint64_t dev_offset);
