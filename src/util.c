#include <stddef.h>
#include <stdint.h>
#include "util.h"

size_t min(size_t a, size_t b) {
  return a < b ? a : b;
}

size_t max(size_t a, size_t b) {
  return a > b ? a : b;
}

void min_in_place_u16(uint16_t* a, uint16_t b) {
  if (*a > b) {
    *a = b;
  }
}

void max_in_place_u16(uint16_t* a, uint16_t b) {
  if (*a < b) {
    *a = b;
  }
}

void max_in_place_u32(uint32_t* a, uint32_t b) {
  if (*a < b) {
    *a = b;
  }
}

// `b` must not be zero.
size_t uint_divide_ceil(size_t a, size_t b) {
  // This is fast because most CPUs include the remainder of a division in its result.
  return (a / b) + (a % b != 0);
}
