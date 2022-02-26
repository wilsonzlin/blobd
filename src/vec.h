#pragma once

#include <immintrin.h>
#include <stdint.h>

typedef union {
  __m128i vec;
  uint8_t elems[16];
} vec_128i_u8_t;

typedef union {
  __m128i vec;
  uint16_t elems[8];
} vec_128i_u16_t;

typedef union {
  __m256i vec;
  uint8_t elems[32];
} vec_256i_u8_t;

typedef union {
  __m512i vec;
  uint8_t elems[64];
} vec_512i_u8_t;

typedef union {
  __m512i vec;
  uint16_t elems[32];
} vec_512i_u16_t;

typedef union {
  __m512i vec;
  uint32_t elems[16];
} vec_512i_u32_t;

typedef struct {
  vec_512i_u8_t range_boundaries;
  vec_512i_u8_t range_lengths;
} vec_sequences_64_t;

typedef struct {
  vec_128i_u8_t range_boundaries;
  vec_128i_u8_t range_lengths;
} vec_sequences_16_t;

vec_512i_u8_t vec_find_indices_of_nonzero_bits_64(uint64_t bits);

vec_128i_u8_t vec_find_indices_of_nonzero_bits_16(uint16_t bits);

vec_sequences_64_t vec_find_set_bit_sequences_in_bitmap_64(uint64_t bitmap);

vec_sequences_16_t vec_find_set_bit_sequences_in_bitmap_16(uint16_t bitmap);
