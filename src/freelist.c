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
#include "inode.h"
#include "log.h"
#include "tile.h"
#include "util.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("freelist");

freelist_t* freelist_create_from_disk_state(device_t* dev, uint64_t dev_offset) {
  cursor_t* cur = dev->mmap + dev_offset;

  // `malloc` aligns on word size (i.e. 8), but we need to align on __m512i.
  freelist_t* fl = aligned_alloc(64, sizeof(freelist_t));
  memset(fl, 0, sizeof(freelist_t));
  fl->dev_offset = dev_offset;
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&fl->rwlock, NULL), "initialise lock on freelist");

  ts_log(INFO, "Loading %zu tiles", dev->tile_count);
  for (uint64_t tile_no = 0; tile_no < dev->tile_count; tile_no++) {
    uint32_t raw = consume_u24(&cur);
    uint64_t itmp = tile_no;
    uint64_t m6 = itmp % 16; itmp /= 16;
    uint64_t m5 = itmp % 16; itmp /= 16;
    uint64_t m4 = itmp % 16; itmp /= 16;
    uint64_t m3 = itmp % 16; itmp /= 16;
    uint64_t m2 = itmp % 16; itmp /= 16;
    uint64_t m1 = itmp;
    if (raw >= 16777214) {
      if (raw == 16777215) {
        uint64_t itmp = tile_no;
        uint64_t i4 = itmp % 64; itmp /= 64;
        uint64_t i3 = itmp % 64; itmp /= 64;
        uint64_t i2 = itmp % 64; itmp /= 64;
        uint64_t i1 = itmp;
        fl->tile_bitmap_4[i1][i2][i3] |= (1llu << i4);
      }
      // Special marking to show that the tile is not a microtile, so we don't write its used size when flushing.
      // NOTE: For out-of-range tiles, we don't touch them, so they may have weird values in the range [0, 15].
      fl->microtile_free_map_6[m1][m2][m3][m4][m5].elems[m6] = (1 << 7) | m6;
    } else {
      fl->microtile_free_map_6[m1][m2][m3][m4][m5].elems[m6] = (raw << 8) | m6;
    }
  }

  ts_log(INFO, "Creating tile bitmaps");
  __m512i zeroes = _mm512_setzero_si512();
  for (uint64_t i1 = 0; i1 < 64; i1 += 8) {
    for (uint64_t i2 = 0; i2 < 64; i2 += 8) {
      for (uint64_t i3 = 0; i3 < 64; i3 += 8) {
        uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_4[i1][i2][i3]), zeroes);
        fl->tile_bitmap_3[i1][i2] |= m << i3;
      }
      uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_3[i1][i2]), zeroes);
      fl->tile_bitmap_2[i1] |= m << i2;
    }
    uint64_t m = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(&fl->tile_bitmap_3[i1]), zeroes);
    fl->tile_bitmap_1 |= m << i1;
  }

  ts_log(INFO, "Creating microtile data structures");
  for (uint32_t i1 = 0; i1 < 16; i1++) {
    for (uint32_t i2 = 0; i2 < 16; i2++) {
      for (uint32_t i3 = 0; i3 < 16; i3++) {
        for (uint32_t i4 = 0; i4 < 16; i4++) {
          for (uint32_t i5 = 0; i5 < 16; i5++) {
            uint32_t m = _mm512_reduce_max_epu32(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec);
            fl->microtile_free_map_5[i1][i2][i3][i4].elems[i5] = (m & ~15) | i5;
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

  ts_log(INFO, "Loaded freelist");

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
  uint64_t shift_avail = highest_bit_idx + 1;
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
  uint64_t itmp = tile;
  fl->dirty_tiles_bitmap_4[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_3[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_2[itmp / 64] |= (1llu << (itmp % 64)); itmp /= 64;
  fl->dirty_tiles_bitmap_1 |= (1llu << itmp);
}

static inline void propagate_tile_bitmap_change(freelist_t* fl, uint64_t new_bitmap, uint64_t i1, uint64_t i2, uint64_t i3) {
  if (!(fl->tile_bitmap_4[i1][i2][i3] = new_bitmap)) {
    if (!(fl->tile_bitmap_3[i1][i2] &= ~(1llu << i3))) {
      if (!(fl->tile_bitmap_2[i1] &= ~(1llu << i2))) {
        fl->tile_bitmap_1 &= ~(1llu << i1);
      }
    }
  }
}

uint32_t fast_allocate_one_tile(freelist_t* fl) {
  uint64_t i1 = _tzcnt_u64(fl->tile_bitmap_1);
  if (i1 == 64) {
    ts_log(CRIT, "Failed to allocate one tile");
    exit(EXIT_NO_SPACE);
  }
  // TODO assert i2 <= 63;
  uint64_t i2 = _tzcnt_u64(fl->tile_bitmap_2[i1]);
  // TODO assert i3 <= 63;
  uint64_t i3 = _tzcnt_u64(fl->tile_bitmap_3[i1][i2]);
  // TODO assert i4 <= 63;
  uint64_t i4 = _tzcnt_u64(fl->tile_bitmap_4[i1][i2][i3]);

  propagate_tile_bitmap_change(fl, fl->tile_bitmap_4[i1][i2][i3] & ~(1llu << i4), i1, i2, i3);
  uint32_t tile = (((((i1 * 64) + i2) * 64) + i3) * 64) + i4;
  mark_tile_as_dirty(fl, tile);

  return tile;
}

// Lock must be already acquired.
void freelist_replenish_tiles_of_inode(freelist_t* fl, cursor_t* inode_cur) {
  uint8_t key_len = inode_cur[INO_OFFSETOF_KEY_LEN];
  uint64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
  ino_last_tile_mode_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];
  uint64_t tile_count = size / TILE_SIZE + (ltm == INO_LAST_TILE_MODE_TILE ? 1 : 0);
  cursor_t* tiles_cur = inode_cur + INO_OFFSETOF_TILES(key_len);
  for (uint64_t i = 0; i < tile_count; i++) {
    uint32_t tile_no = consume_u24(&tiles_cur);
    uint64_t itmp = tile_no;
    uint64_t i4 = itmp % 64; itmp /= 64;
    uint64_t i3 = itmp % 64; itmp /= 64;
    uint64_t i2 = itmp % 64; itmp /= 64;
    uint64_t i1 = itmp % 64;
    fl->tile_bitmap_4[i1][i2][i3] |= 1llu << i4;
    mark_tile_as_dirty(fl, tile_no);
  }
}

// tiles_needed must be nonzero.
void freelist_consume_tiles(freelist_t* fl, uint64_t tiles_needed, cursor_t* out) {
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&fl->rwlock), "acquire write lock on freelist");

  uint64_t tiles_needed_orig = tiles_needed;

  vec_512i_u8_t i1_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_1);
  for (uint64_t o1 = 0, i1; (i1 = i1_candidates.elems[o1]) != 64; o1++) {
    vec_512i_u8_t i2_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_2[i1]);
    for (uint64_t o2 = 0, i2; (i2 = i2_candidates.elems[o2]) != 64; o2++) {
      vec_512i_u8_t i3_candidates = vec_find_indices_of_nonzero_bits_64(fl->tile_bitmap_3[i1][i2]);
      for (uint64_t o3 = 0, i3; (i3 = i3_candidates.elems[o3]) != 64; o3++) {
        free_tiles_t result = find_free_tiles_in_region(fl->tile_bitmap_4[i1][i2][i3], min(tiles_needed, 64));
        for (uint64_t i = 0; result.tiles.elems[i] != 64; i++) {
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

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&fl->rwlock), "release write lock on freelist");

  DEBUG_TS_LOG("Allocated %zu tiles", tiles_needed_orig);
}

uint32_t freelist_consume_one_tile(freelist_t* fl) {
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&fl->rwlock), "acquire write lock on freelist");

  uint32_t tile = fast_allocate_one_tile(fl);

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&fl->rwlock), "release write lock on freelist");

  return tile;
}

freelist_consumed_microtile_t freelist_consume_microtiles(freelist_t* fl, uint32_t bytes_needed) {
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&fl->rwlock), "acquire write lock on freelist");

  __m512i y512 = _mm512_set1_epi32(bytes_needed << 8);

  uint32_t m1 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_1.vec, y512);
  uint32_t p1 = _tzcnt_u32(m1);
  uint32_t microtile_addr;
  uint32_t cur_free;
  // Use uint32_t as we'll multiply these.
  uint32_t i1, i2, i3, i4, i5, i6;
  if (p1 == 32) {
    // There are no microtiles, so allocate a new one.
    microtile_addr = fast_allocate_one_tile(fl);
    DEBUG_TS_LOG("No existing microtile available, so converting tile %u", microtile_addr);
    cur_free = TILE_SIZE;
    uint32_t itmp = microtile_addr;
    i6 = itmp % 16; itmp /= 16;
    i5 = itmp % 16; itmp /= 16;
    i4 = itmp % 16; itmp /= 16;
    i3 = itmp % 16; itmp /= 16;
    i2 = itmp % 16; itmp /= 16;
    i1 = itmp % 16;
  } else {
    DEBUG_ASSERT_STATE(p1 <= 15, "invalid microtile free map p1 %u", p1);
    i1 = fl->microtile_free_map_1.elems[p1] & 127;
    DEBUG_ASSERT_STATE(i1 <= 15, "invalid microtile free map i1 %u", i1);
    uint16_t m2 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_2[i1].vec, y512);
    uint32_t p2 = _tzcnt_u32(m2);
    DEBUG_ASSERT_STATE(p2 <= 15, "invalid microtile free map p2 %u", p2);

    i2 = fl->microtile_free_map_2[i1].elems[p2] & 127;
    DEBUG_ASSERT_STATE(i2 <= 15, "invalid microtile free map i2 %u", i2);
    uint16_t m3 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_3[i1][i2].vec, y512);
    uint32_t p3 = _tzcnt_u32(m3);
    DEBUG_ASSERT_STATE(p3 <= 15, "invalid microtile free map p3 %u", p3);

    i3 = fl->microtile_free_map_3[i1][i2].elems[p3] & 127;
    DEBUG_ASSERT_STATE(i3 <= 15, "invalid microtile free map i3 %u", i3);
    uint16_t m4 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_4[i1][i2][i3].vec, y512);
    uint32_t p4 = _tzcnt_u32(m4);
    DEBUG_ASSERT_STATE(p4 <= 15, "invalid microtile free map p4 %u", p4);

    i4 = fl->microtile_free_map_4[i1][i2][i3].elems[p4] & 127;
    DEBUG_ASSERT_STATE(i4 <= 15, "invalid microtile free map i4 %u", i4);
    uint16_t m5 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_5[i1][i2][i3][i4].vec, y512);
    uint32_t p5 = _tzcnt_u32(m5);
    DEBUG_ASSERT_STATE(p5 <= 15, "invalid microtile free map p5 %u", p5);

    i5 = fl->microtile_free_map_5[i1][i2][i3][i4].elems[p5] & 127;
    DEBUG_ASSERT_STATE(i5 <= 15, "invalid microtile free map i5 %u", i4);
    uint16_t m6 = _mm512_cmpge_epu32_mask(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec, y512);
    uint32_t p6 = _tzcnt_u32(m6);
    DEBUG_ASSERT_STATE(p6 <= 15, "invalid microtile free map p6 %u", p6);

    uint32_t meta = fl->microtile_free_map_6[i1][i2][i3][i4][i5].elems[p6];
    cur_free = meta >> 8;
    i6 = meta & 127;
    DEBUG_ASSERT_STATE(i6 <= 15, "invalid microtile free map i6 %u", i4);
    microtile_addr = (((((((((i1 * 16) + i2) * 16) + i3) * 16) + i4) * 16) + i5) * 16) + i6;
    mark_tile_as_dirty(fl, microtile_addr);
    DEBUG_TS_LOG("Found microtile %u with %u free space", microtile_addr, cur_free);
  }

  freelist_consumed_microtile_t out;
  out.microtile = microtile_addr;
  out.microtile_offset = TILE_SIZE - cur_free;

  // TODO assert cur_usage >= bytes_needed.
  uint32_t new_free = cur_free - bytes_needed;
  DEBUG_TS_LOG("Microtile %u now has %u free space", microtile_addr, new_free);
  fl->microtile_free_map_6[i1][i2][i3][i4][i5].elems[i6] = (new_free << 8) | i6;
  fl->microtile_free_map_5[i1][i2][i3][i4].elems[i5] = (_mm512_reduce_max_epu32(fl->microtile_free_map_6[i1][i2][i3][i4][i5].vec) & ~15) | i5;
  fl->microtile_free_map_4[i1][i2][i3].elems[i4] = (_mm512_reduce_max_epu32(fl->microtile_free_map_5[i1][i2][i3][i4].vec) & ~15) | i4;
  fl->microtile_free_map_3[i1][i2].elems[i3] = (_mm512_reduce_max_epu32(fl->microtile_free_map_4[i1][i2][i3].vec) & ~15) | i3;
  fl->microtile_free_map_2[i1].elems[i2] = (_mm512_reduce_max_epu32(fl->microtile_free_map_3[i1][i2].vec) & ~15) | i2;
  fl->microtile_free_map_1.elems[i1] = (_mm512_reduce_max_epu32(fl->microtile_free_map_2[i1].vec) & ~15) | i1;

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&fl->rwlock), "release write lock on freelist");

  return out;
}
