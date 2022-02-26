#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include "device.h"

/**

BUCKET
======

Structure of bucket pointers
----------------------------

u8 count_log2
{
  {
    u24 microtile
    u24 microtile_byte_offset
  }[16] pointers
  u64 xxhash
}[] sixteen_pointers

**/

typedef struct {
  size_t dev_offset_pointers;
  // For simplicity, we use u64 values. Bits [24:47] represent microtile, [0:23] microtile byte offset; the other bits are unused.
  atomic_uint_least64_t* bucket_pointers;
  uint64_t** dirty_sixteen_pointers;
  size_t dirty_sixteen_pointers_layer_count;
  pthread_rwlock_t dirty_sixteen_pointers_rwlock;
  uint8_t count_log2;
} buckets_t;

buckets_t* buckets_create_from_disk_state(device_t* dev, size_t dev_offset);
