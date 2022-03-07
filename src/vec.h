#pragma once

#include <immintrin.h>
#include <stdint.h>

typedef struct {
  uint8_t elems[64];
} array_u8_64_t;

typedef union {
  __m512i vec;
  uint8_t elems[64];
} vec_512i_u8_t;

typedef union {
  __m512i vec;
  uint32_t elems[16];
} vec_512i_u32_t;

typedef struct {
  vec_512i_u8_t range_boundaries;
  vec_512i_u8_t range_lengths;
} vec_sequences_64_t;

array_u8_64_t vec_find_indices_of_nonzero_bits_64(uint64_t bits);

// WARNING: UNIQUE_VAR_INNER_2 is necessary (check with `gcc -E` if not believed).
#define UNIQUE_VAR_INNER_2(base, x) base ## x
#define UNIQUE_VAR_INNER(base, x) UNIQUE_VAR_INNER_2(base, x)
#define UNIQUE_VAR(base) UNIQUE_VAR_INNER(base, __COUNTER__)

#define __VEC_ITER_INDICES_OF_NONZERO_BITS_64_WITH_VAR(indices, out, var_i) \
  for (uint64_t var_i = 0, out; var_i < 64 && (out = indices.elems[var_i]) != 64; var_i++)

#define VEC_ITER_INDICES_OF_NONZERO_BITS_64(indices, out) __VEC_ITER_INDICES_OF_NONZERO_BITS_64_WITH_VAR(indices, out, UNIQUE_VAR(__viionb64)) \

vec_sequences_64_t vec_find_set_bit_sequences_in_bitmap_64(uint64_t bitmap);
