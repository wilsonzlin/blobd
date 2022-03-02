#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include "device.h"

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

typedef struct {
  pthread_rwlock_t lock;
  uint32_t microtile;
  uint32_t microtile_byte_offset;
} bucket_t;

typedef struct {
  size_t dev_offset_pointers;
  bucket_t* buckets;
  uint64_t** dirty_sixteen_pointers;
  size_t dirty_sixteen_pointers_layer_count;
  pthread_rwlock_t dirty_sixteen_pointers_rwlock;
  uint8_t count_log2;
} buckets_t;

buckets_t* buckets_create_from_disk_state(device_t* dev, size_t dev_offset);
