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
  int fd;
  cursor_t* mmap;
  // Exact byte length of block device.
  uint64_t size;
  // This should equal ceil(device_size / tile_size).
  uint32_t tile_count;
  uint64_t page_mask;
} device_t;

device_t* device_create(int fd, void* mmap, uint64_t size, uint64_t page_size);

void device_sync(device_t* dev, uint64_t start, uint64_t end_exclusive);

void device_format(device_t* dev, uint8_t bucket_count_log2);
