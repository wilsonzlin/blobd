#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>
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

// `n` must be nonzero.
// Returns -1 on error or close, 0 on not ready, and nonzero on (partial) read.
int maybe_read(int fd, uint8_t* out_buf, size_t n) {
  int readno = read(fd, out_buf, n);
  if (readno == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    return -1;
  }
  if (!readno) {
    return -1;
  }
  return readno;
}

// `n` must be nonzero.
// Returns -1 on error or close, 0 on not ready, and nonzero on (partial) read.
int maybe_write(int fd, uint8_t* in_buf, size_t n) {
  int writeno = write(fd, in_buf, n);
  if (writeno == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    return -1;
  }
  if (!writeno) {
    return -1;
  }
  return writeno;
}
