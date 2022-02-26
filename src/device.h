#pragma once

#include <stddef.h>
#include "cursor.h"

/**

DEVICE
======

Structure
---------

journal
freelist
buckets
heap

**/

typedef struct {
  cursor_t* mmap;
  // Exact byte length of block device.
  size_t size;
  // This should equal ceil(device_size / tile_size).
  uint32_t tile_count;
} device_t;

device_t* device_create(void* mmap, size_t size);
