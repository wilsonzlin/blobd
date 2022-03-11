#pragma once

#include "exit.h"
#include "log.h"
#include <immintrin.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define ASSERT_ERROR_RETVAL_OK(x, msg, ...) \
  { \
    int __xresult = x; \
    if (__xresult) { \
      ts_log(CRIT, "Failed to %s with error %d", msg, ##__VA_ARGS__, __xresult); \
      exit(EXIT_INTERNAL); \
    } \
  }

#define ASSERT_STATE(chk, msg, ...) \
  if (!(chk)) { \
    ts_log(CRIT, "State check failed: " msg, ##__VA_ARGS__); \
    exit(EXIT_INTERNAL); \
  }

#ifdef TURBOSTORE_DEBUG
#define DEBUG_ASSERT_STATE(chk, msg, ...) ASSERT_STATE(chk, msg, ##__VA_ARGS__)
#else
#define DEBUG_ASSERT_STATE(chk, msg, ...) ((void) 0)
#endif

#ifdef TURBOSTORE_DEBUG
#define DEBUG_TS_LOG(msg, ...) ts_log(DEBUG, msg, ##__VA_ARGS__)
#else
#define DEBUG_TS_LOG(msg, ...) ((void) 0)
#endif

uint64_t min(uint64_t a, uint64_t b);

uint64_t max(uint64_t a, uint64_t b);

void min_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u32(uint32_t* a, uint32_t b);

uint64_t uint_divide_ceil(uint64_t a, uint64_t b);

int maybe_read(int fd, uint8_t* out_buf, uint64_t n);

int maybe_write(int fd, uint8_t* in_buf, uint64_t n);
