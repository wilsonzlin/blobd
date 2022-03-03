#define _GNU_SOURCE

#include <immintrin.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "freelist.h"
#include "log.h"
#include "tile.h"
#include "util.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("freelist");

freelist_t* freelist_create_from_disk_state(device_t* dev, size_t dev_offset) {
  cursor_t* cur = dev->mmap + dev_offset;

  // `malloc` aligns on word size (i.e. 8), but we need to align on __m512i.
  freelist_t* fl = aligned_alloc(64, sizeof(freelist_t));
  memset(fl, 0, sizeof(freelist_t));
  fl->dev_offset = dev_offset;
  if (pthread_rwlock_init(&fl->rwlock, NULL)) {
    perror("Failed to initialise lock on freelist");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "Loading %zu tiles", dev->tile_count);
  for (size_t tile_no = 0; tile_no < dev->tile_count; tile_no++) {
    uint32_t raw = consume_u24(&cur);
    size_t itmp = tile_no;
    size_t m6 = itmp % 16; itmp /= 16;
    size_t m5 = itmp % 16; itmp /= 16;
    size_t m4 = itmp % 16; itmp /= 16;
    size_t m3 = itmp % 16; itmp /= 16;
    size_t m2 = itmp % 16; itmp /= 16;
    size_t m1 = itmp;
    if (raw >= 16777214) {
      if (raw == 16777215) {
        size_t itmp = tile_no;
        size_t i4 = itmp % 64; itmp /= 64;
        size_t i3 = itmp % 64; itmp /= 64;
        size_t i2 = itmp % 64; itmp /= 64;
        size_t i1 = itmp;
        fl->tile_bitmap_4[i1][i2][i3] |= (1llu << i4);
      }
      // Special marking to show that the tile is not a microtile, so we don't write its used size when flushing.
      fl->microtile_free_map_6[m1][m2][m3][m4][m5].elems[m6] = 16;
    } else {
      fl->microtile_free_map_6[m1][m2][m3][m4][m5].elems[m6] = (raw << 8) | m6;
    }
  }

  ts_log(DEBUG, "Creating tile bitmaps");
  __m512i zeroes = _mm512_setzero_si512();
  for (size_t i1 = 0; i1 < 64; i1 += 8) {
    for (size_t i2 = 0; i2 < 64; i2 += 8) {
      for (size_t i3 = 0; i3 < 64; i3 += 8) {
        uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_4[i1][i2][i3]), zeroes);
        fl->tile_bitmap_3[i1][i2] |= m << i3;
      }
      uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_3[i1][i2]), zeroes);
      fl->tile_bitmap_2[i1] |= m << i2;
    }
    uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_3[i1]), zeroes);
    fl->tile_bitmap_1 |= m << i1;
  }

  ts_log(DEBUG, "Creating microtile data structures");
  for (size_t i1 = 0; i1 < 16; i1++) {
    for (size_t i2 = 0; i2 < 16; i2++) {
      for (size_t i3 = 0; i3 < 16; i3++) {
        for (size_t i4 = 0; i4 < 16; i4++) {
          for (size_t i5 = 0; i5 < 16; i5++) {
            uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec);
            fl->microtile_free_map_5[i1][i2][i3][i4].elems[i5] = m;
          }
          uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_5[i1][i2][i3][i4].vec);
          fl->microtile_free_map_4[i1][i2][i3].elems[i4] = (m & ~15) | i4;
        }
        uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_4[i1][i2][i3].vec);
        fl->microtile_free_map_3[i1][i2].elems[i3] = (m & ~15) | i3;
      }
      uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_3[i1][i2].vec);
      fl->microtile_free_map_2[i1].elems[i2] = (m & ~15) | i2;
    }
    uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_2[i1].vec);
    fl->microtile_free_map_1.elems[i1] = (m & ~15) | i1;
  }

  ts_log(DEBUG, "Loaded freelist");

  return fl;
}

typedef struct {
  vec_512i_u8_t tiles;
  uint64_t new_bitmap;
} free_tiles_t;

// `bitmap` must be nonzero.
// `want` must be in the range [1, 64].
static inline free_tiles_t find_free_tiles_in_region(uint64_t region_tile_bitmap, uint8_t tile_count_wanted) {
  uint8_t indices_raw[64] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63};
  __m512i indices = _mm512_loadu_epi8(indices_raw);
  __m512i fill = _mm512_set1_epi8(64);

  // Find tiles that are available.
  vec_512i_u8_t available;
  available.vec = _mm512_mask_compress_epi8(fill, region_tile_bitmap, indices);
  // Get index of highest tile found.
  uint8_t highest_bit_idx = min(63 - _lzcnt_u64(region_tile_bitmap), available.elems[(tile_count_wanted - 1)]);
  // We use shifts as building a mask using ((1 << (highest_bit_idx + 1)) - 1) requires special handling if highest_bit_idx is 63, and the same for ~(0x8000000000000000 >> (63 - highest_bit_idx)).
  size_t shift_avail = highest_bit_idx + 1;
  uint64_t new_bitmap = (region_tile_bitmap >> shift_avail) << shift_avail;
  // Limit tile count to tile_count_wanted.
  available.vec = _mm512_mask_blend_epi8((1llu << tile_count_wanted) - 1, fill, available.vec);
  free_tiles_t result = {
    .tiles = available,
    .new_bitmap = new_bitmap,
  };
  return result;
}

static inline void mark_tile_as_dirty(freelist_t* fl, uint32_t tile) {
  size_t itmp = tile;
  fl->dirty_tiles_bitmap_4[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_3[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_2[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_1 |= (1llu << itmp);
}

static inline void propagate_tile_bitmap_change(freelist_t* fl, uint64_t new_bitmap, size_t i1, size_t i2, size_t i3) {
  if (!(fl->tile_bitmap_4[i1][i2][i3] = new_bitmap)) {
    if (!(fl->tile_bitmap_3[i1][i2] &= ~(1llu << i3))) {
      if (!(fl->tile_bitmap_2[i1] &= ~(1llu << i2))) {
        fl->tile_bitmap_1 &= ~(1llu << i1);
      }
    }
  }
}

uint32_t fast_allocate_one_tile(freelist_t* fl) {
  size_t i1 = _tzcnt_u64(fl->tile_bitmap_1);
  if (i1 == 64) {
    ts_log(CRIT, "Failed to allocate one tile");
    exit(EXIT_NO_SPACE);
  }
  // TODO assert i2 <= 63;
  size_t i2 = _tzcnt_u64(fl->tile_bitmap_2[i1]);
  // TODO assert i3 <= 63;
  size_t i3 = _tzcnt_u64(fl->tile_bitmap_3[i1][i2]);
  // TODO assert i4 <= 63;
  size_t i4 = _tzcnt_u64(fl->tile_bitmap_4[i1][i2][i3]);

  propagate_tile_bitmap_change(fl, fl->tile_bitmap_4[i1][i2][i3] & ~(1llu << i4), i1, i2, i3);
  uint32_t tile = (((((i1 * 64) + i2) * 64) + i3) * 64) + i4;
  mark_tile_as_dirty(fl, tile);

  return tile;
}

// tiles_needed must be nonzero.
void freelist_consume_tiles(freelist_t* fl, size_t tiles_needed, cursor_t* out) {
  if (pthread_rwlock_wrlock(&fl->rwlock)) {
    perror("Failed to acquire write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  size_t tiles_needed_orig = tiles_needed;

  vec_512i_u8_t i1_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_1);
  for (size_t o1 = 0, i1; (i1 = i1_candidates.elems[o1]) != 64; o1++) {
    vec_512i_u8_t i2_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_2[i1]);
    for (size_t o2 = 0, i2; (i2 = i2_candidates.elems[o2]) != 64; o2++) {
      vec_512i_u8_t i3_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_3[i1][i2]);
      for (size_t o3 = 0, i3; (i3 = i3_candidates.elems[o3]) != 64; o3++) {
        free_tiles_t result = find_free_tiles_in_region(fl->tile_bitmap_4[i1][i2][i3], min(tiles_needed, 64));
        for (size_t i = 0; result.tiles.elems[i] != 64; i++) {
          uint32_t tile = (((((i1 * 64) + i2) * 64) + i3) * 64) + result.tiles.elems[i];
          produce_u24(&out, tile);
          mark_tile_as_dirty(fl, tile);
          tiles_needed--;
        }
        propagate_tile_bitmap_change(fl, result.new_bitmap, i1, i2, i3);

        if (!tiles_needed) {
          goto postloop;
        }
      }
    }
  }

  postloop:
  if (tiles_needed) {
    ts_log(CRIT, "Failed to allocate %zu of %zu requested tiles", tiles_needed, tiles_needed_orig);
    exit(EXIT_NO_SPACE);
  }

  if (pthread_rwlock_unlock(&fl->rwlock)) {
    perror("Failed to release write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  ts_log(DEBUG, "Allocated %zu tiles", tiles_needed_orig);
}

uint32_t freelist_consume_one_tile(freelist_t* fl) {
  if (pthread_rwlock_wrlock(&fl->rwlock)) {
    perror("Failed to acquire write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  uint32_t tile = fast_allocate_one_tile(fl);

  if (pthread_rwlock_unlock(&fl->rwlock)) {
    perror("Failed to release write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  return tile;
}

freelist_consumed_microtile_t freelist_consume_microtiles(freelist_t* fl, size_t bytes_needed) {
  if (pthread_rwlock_wrlock(&fl->rwlock)) {
    perror("Failed to acquire write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  __m512i y512 = _mm512_set1_epi32(bytes_needed << 8);

  uint32_t m1 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_1.vec, y512);
  uint32_t p1 = _tzcnt_u32(m1);
  uint32_t microtile_addr;
  uint32_t cur_free;
  size_t i1, i2, i3, i4, i5, i6;
  if (p1 == 32) {
    // There are no microtiles, so allocate a new one.
    microtile_addr = fast_allocate_one_tile(fl);
    ts_log(DEBUG, "Created a new microtile at %u", microtile_addr);
    cur_free = TILE_SIZE;
    size_t itmp = microtile_addr;
    i6 = itmp % 16; itmp /= 16;
    i5 = itmp % 16; itmp /= 16;
    i4 = itmp % 16; itmp /= 16;
    i3 = itmp % 16; itmp /= 16;
    i2 = itmp % 16; itmp /= 16;
    i1 = itmp % 16;
  } else {
    i1 = fl->microtile_free_map_1.elems[p1] & 255;
    uint8_t m2 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_2[i1].vec, y512);
    // TODO Assert p2 <= 15
    uint32_t p2 = _tzcnt_u32(m2);

    i2 = fl->microtile_free_map_2[i1].elems[p2] & 255;
    uint8_t m3 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_3[i1][i2].vec, y512);
    // TODO Assert p3 <= 15
    uint32_t p3 = _tzcnt_u32(m3);

    i3 = fl->microtile_free_map_3[i1][i2].elems[p3] & 255;
    uint8_t m4 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_4[i1][i2][i3].vec, y512);
    // TODO Assert p4 <= 15
    uint32_t p4 = _tzcnt_u32(m4);

    i4 = fl->microtile_free_map_4[i1][i2][i3].elems[p4] & 255;
    uint8_t m5 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_5[i1][i2][i3][i4].vec, y512);
    // TODO Assert p5 <= 15
    uint32_t p5 = _tzcnt_u32(m5);

    i5 = fl->microtile_free_map_5[i1][i2][i3][i4].elems[p5] & 255;
    uint8_t m6 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec, y512);
    // TODO Assert p6 <= 15
    uint32_t p6 = _tzcnt_u32(m6);

    uint32_t meta = fl->microtile_free_map_6[i1][i2][i3][i4][i5].elems[p6];
    cur_free = meta >> 8;
    i6 = meta & 255;
    microtile_addr = (((((((((i1 * 16) + i2) * 16) + i3) * 16) + i4) * 16) + i5) * 16) + i6;
  }

  freelist_consumed_microtile_t out;
  out.microtile = microtile_addr;
  out.microtile_offset = TILE_SIZE - cur_free;

  // TODO assert cur_usage >= bytes_needed.
  uint32_t new_free = cur_free - bytes_needed;
  fl->microtile_free_map_6[i1][i2][i3][i4][i5].elems[i6] = (new_free << 8) | i6;
  fl->microtile_free_map_5[i1][i2][i3][i4].elems[i5] = _mm512_reduce_max_epu32(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec);
  fl->microtile_free_map_4[i1][i2][i3].elems[i4] = (_mm512_reduce_max_epu32(fl->microtile_free_map_5[i1][i2][i3][i4].vec) & ~15) | i5;
  fl->microtile_free_map_3[i1][i2].elems[i3] = (_mm512_reduce_max_epu32(fl->microtile_free_map_4[i1][i2][i3].vec) & ~15) | i4;
  fl->microtile_free_map_2[i1].elems[i2] = (_mm512_reduce_max_epu32(fl->microtile_free_map_3[i1][i2].vec) & ~15) | i3;
  fl->microtile_free_map_1.elems[i1] = (_mm512_reduce_max_epu32(fl->microtile_free_map_2[i1].vec) & ~15) | i2;

  if (pthread_rwlock_unlock(&fl->rwlock)) {
    perror("Failed to release write lock on freelist");
    exit(EXIT_INTERNAL);
  }

  return out;
}
