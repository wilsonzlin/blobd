#pragma once

#include <pthread.h>
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
