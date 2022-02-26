#include <stddef.h>
#include <stdlib.h>
#include "device.h"
#include "tile.h"
#include "util.h"

device_t* device_create(void* mmap, size_t size) {
  device_t* dev = malloc(sizeof(device_t));
  dev->mmap = mmap;
  dev->size = size;
  dev->tile_count = uint_divide_ceil(size, TILE_SIZE);
  return dev;
}
