#include <immintrin.h>
#include <stdint.h>
#include <string.h>
#include "vec.h"

array_u8_64_t vec_find_indices_of_nonzero_bits_64(uint64_t bits) {
  uint8_t indices_raw[64] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63};
  __m512i indices = _mm512_loadu_epi8(indices_raw);

  array_u8_64_t result;
  memset(result.elems, 64, 64);
  _mm512_mask_compressstoreu_epi8(result.elems, bits, indices);
  return result;
}

vec_sequences_64_t vec_find_set_bit_sequences_in_bitmap_64(uint64_t bitmap) {
  uint8_t indices_raw[64] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63};
  __m512i indices = _mm512_loadu_epi8(indices_raw);
  __m512i fill = _mm512_set1_epi8(64);

  uint64_t range_boundaries_mask = bitmap ^ (bitmap << 1);
  vec_512i_u8_t range_boundaries;
  range_boundaries.vec = _mm512_mask_compress_epi8(fill, range_boundaries_mask, indices);
  vec_512i_u8_t range_lengths;
  // TODO Is directly using right shift operator (instead of intrinsic) on __m512i slow?
  range_lengths.vec = _mm512_sub_epi8((range_boundaries.vec >> 8), range_boundaries.vec);
  vec_sequences_64_t result = {
    .range_boundaries = range_boundaries,
    .range_lengths = range_lengths,
  };
  return result;
}
