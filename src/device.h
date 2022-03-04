#pragma once

#include <stdint.h>
#include "cursor.h"

/**

DEVICE
======

Structure
---------

journal
stream
freelist
buckets
heap

**/

typedef struct {
  cursor_t* mmap;
  // Exact byte length of block device.
  uint64_t size;
  // This should equal ceil(device_size / tile_size).
  uint32_t tile_count;
} device_t;

device_t* device_create(void* mmap, uint64_t size);

void device_sync(device_t* dev);

void device_format(device_t* dev, uint8_t bucket_count_log2);
