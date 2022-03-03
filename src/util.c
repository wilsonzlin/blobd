#include <errno.h>
#include <immintrin.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include "util.h"

uint64_t min(uint64_t a, uint64_t b) {
  return a < b ? a : b;
}

uint64_t max(uint64_t a, uint64_t b) {
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
uint64_t uint_divide_ceil(uint64_t a, uint64_t b) {
  // This is fast because most CPUs include the remainder of a division in its result.
  return (a / b) + (a % b != 0);
}

// `n` must be nonzero.
// Returns -1 on error or close, 0 on not ready, and nonzero on (partial) read.
int maybe_read(int fd, uint8_t* out_buf, uint64_t n) {
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
int maybe_write(int fd, uint8_t* in_buf, uint64_t n) {
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

// `b_upper` and `b_lower` must be filled with 0.
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, __m512i b_lower, __m512i b_upper) {
  uint8_t ino_key[64];
  memcpy(ino_key, a, min(a_len, 64));
  if (a_len < 64) {
    memset(ino_key, 0, 64 - a_len);
  }
  __m512i ino_key_lower = _mm512_loadu_epi8(ino_key);
  // WARNING: Both __m512i arguments must be filled with the same character.
  if (_mm512_cmpneq_epi8_mask(ino_key_lower, b_lower)) {
    return false;
  }
  if (a_len > 64) {
    memcpy(ino_key, a + 64, a_len - 64);
    memset(ino_key, 0, 128 - (a_len - 64));
    __m512i ino_key_upper = _mm512_loadu_epi8(ino_key);
    // WARNING: Both __m512i arguments must be filled with the same character.
    if (_mm512_cmpneq_epi8_mask(ino_key_upper, b_upper)) {
      return false;
    }
  }
  return true;
}
