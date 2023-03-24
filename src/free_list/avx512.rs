use std::{simd::{u32x16, u8x64, i8x64}, arch::x86_64::{_mm512_loadu_epi8, _mm512_set1_epi8, _mm512_mask_compress_epi8, _mm512_mask_blend_epi8, _tzcnt_u64, _mm512_mask_compressstoreu_epi8, _mm512_set1_epi32, _mm512_cmpge_epu32_mask, _tzcnt_u32, _mm512_reduce_max_epu32, _mm512_setzero_si512, _mm512_cmpneq_epu64_mask, _mm512_loadu_epi64}, cmp::min};

use itertools::Itertools;
use off64::{usz, Off64};
use seekable_async_file::SeekableAsyncFile;

use crate::{tile::TILE_SIZE, ndtreearray::NDTreeArray};

use super::FreeList;

unsafe fn find_indices_of_nonzero_bits_64(bits: u64) -> [u8; 64] {
  const INDICES_RAW: [i8; 64] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63];
  let indices = _mm512_loadu_epi8(INDICES_RAW.as_ptr());

  let mut result = [64u8; 64];
  _mm512_mask_compressstoreu_epi8(result.as_mut_ptr(), bits, indices);
  result
}

unsafe fn find_set_bit_sequences_in_bitmap_64(bitmap: u64) -> impl Iterator<Item = (u64, u64)> {
  const INDICES_RAW: [i8; 64] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63];
  let indices = _mm512_loadu_epi8(INDICES_RAW.as_ptr());
  let fill = _mm512_set1_epi8(64);

  let range_boundaries_mask = bitmap ^ (bitmap << 1);
  let range_boundaries: i8x64 = _mm512_mask_compress_epi8(fill, range_boundaries_mask, indices).into();
  // If we want to get (start, len) pairs instead of (start, end), we can right shift off one least significant i8 element (which is actually the element at index zero/smallest memory address due to Intel little endianness) and subtract `range_boundaries`.
  range_boundaries.to_array().into_iter().map(|idx| u64::try_from(idx).unwrap() ).tuples()
}


// `region_tile_bitmap` must be nonzero.
// `tile_count_wanted` must be in the range [1, 64].
unsafe fn find_free_tiles_in_region(region_tile_bitmap: u64, tile_count_wanted: u8) -> u8x64 {
  const INDICES_RAW: [i8; 64] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63];
  let indices = _mm512_loadu_epi8(INDICES_RAW.as_ptr());
  let fill = _mm512_set1_epi8(64);

  // Find tiles that are available.
  let available = _mm512_mask_compress_epi8(fill, region_tile_bitmap, indices);
  // Limit tile count to tile_count_wanted.
  let available = _mm512_mask_blend_epi8((1u64 << tile_count_wanted) - 1, fill, available);
  available.into()
}

// We need to align on __m512i.
#[repr(align(64))]
pub struct Avx512FreeList {
  dev_offset: u64,
  // Bits are set if they're free.
  tile_bitmaps: NDTreeArray<u64>,
  // Storing free amount (instead of used) allows us to initialise these in-memory structures to empty and not available by default, which is faster and more importantly, ensures that all elements including out-of-bound ones that map to invalid tiles are unavailable by default.
  // For each element (u32), bits [31:8] represent the maximum free amount of all elements in the next layer, [7:7] if the tile is not a microtile, and [6:0] the corresponding index of the maximum in the next layer (although only 4 bits are used as there are only 16 elements per vector).
  microtile_free_maps: NDTreeArray<u32x16>,
}

impl Avx512FreeList {
  pub fn new(dev_offset: u64) -> Self {
    Self {
      dev_offset,
      tile_bitmaps: NDTreeArray::new(64, 4),
      microtile_free_maps: NDTreeArray::new(16, 6),
    }
  }

  fn propagate_tile_bitmap_change(&mut self, new_bitmap: u64, i1: u64, i2: u64, i3:  u64) {
    if self.tile_bitmaps.set([i1, i2, i3], new_bitmap) != 0 {
      return;
    };

    if self.tile_bitmaps.update([i1, i2], |b3| b3 & !(1u64 << i3)) != 0 {
      return;
    };

    if self.tile_bitmaps.update([i1], |b2| b2 & !(1u64 << i2)) != 0 {
      return;
    };

    self.tile_bitmaps.compute_root(|b1| b1 & !(1u64 << i1));
  }

  unsafe fn fast_allocate_one_tile(&mut self) -> u32 {
    let i1 = _tzcnt_u64(self.tile_bitmaps.root());
    if i1 == 64 {
      panic!("failed to allocate one tile, most likely out of space");
    };
    let i2 = _tzcnt_u64(self.tile_bitmaps.get([i1]));
    assert!(i2 <= 63);
    let i3 = _tzcnt_u64(self.tile_bitmaps.get([i1,i2]));
    assert!(i3 <= 63);
    let i4 = _tzcnt_u64(self.tile_bitmaps.get([i1,i2,i3]));
    assert!(i4 <= 63);

    self.propagate_tile_bitmap_change(self.tile_bitmaps.get([i1, i2, i3]) & !(1u64 << i4), i1, i2, i3);

    let tile = (((((i1 * 64) + i2) * 64) + i3) * 64) + i4;
    tile.try_into().unwrap()
  }
}

impl FreeList for Avx512FreeList {
  fn load_from_device(&mut self, device: SeekableAsyncFile, tile_count: u64) {
    eprintln!("Loading {} tiles", tile_count);
    let mut free_tile_count = 0u64;
    let mut microtile_count = 0u64;
    let mut cur = self.dev_offset;
    for tile_no in 0..tile_count {
      let raw = device.read_at_sync(cur, 3).read_u24_be_at(0);
      cur += 3;
      let mut itmp = tile_no;
      let m6 = u32::try_from(itmp % 16).unwrap(); itmp /= 16;
      let m5 = itmp % 16; itmp /= 16;
      let m4 = itmp % 16; itmp /= 16;
      let m3 = itmp % 16; itmp /= 16;
      let m2 = itmp % 16; itmp /= 16;
      let m1 = itmp;
      if raw >= 16777214 {
        if raw == 16777215 {
          free_tile_count += 1;
          let mut itmp = tile_no;
          let i4 = itmp % 64; itmp /= 64;
          let i3 = itmp % 64; itmp /= 64;
          let i2 = itmp % 64; itmp /= 64;
          let i1 = itmp;
          self.tile_bitmaps.update([i1, i2, i3], |b| b | (1u64 << i4));
        }
        // Special marking to show that the tile is not a microtile, so we don't write its used size when flushing.
        // NOTE: For out-of-range tiles, we don't touch them, so they may have weird values in the range [0, 15].
        self.microtile_free_maps.get_mut([m1,m2,m3,m4,m5])[usz!(m6)] = (1 << 7) | m6;
      } else {
        microtile_count += 1;
        self.microtile_free_maps.get_mut([m1,m2,m3,m4,m5])[usz!(m6)] = (raw << 8) | m6;
      }
    }

    eprintln!("{} free tiles, {} microtiles", free_tile_count, microtile_count);

    unsafe {
      eprintln!("Creating tile bitmaps");
      let zeroes = _mm512_setzero_si512();
      for i1 in 0u64..64 {
        for i2 in 0u64..64 {
          for i3 in (0u64..64).step_by(8) {
            let m: u64 = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(
              self.tile_bitmaps.get_ref([i1,i2,i3]) as *const u64 as *const i64
            ), zeroes).into();
            self.tile_bitmaps.update([i1,i2], |b| b | (m << i3));
          }
        }
      }
      for i1 in 0u64..64 {
        for i2 in (0u64..64).step_by(8) {
          let m: u64 = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(
            self.tile_bitmaps.get_ref([i1,i2]) as *const u64 as *const i64
          ), zeroes).into();
          self.tile_bitmaps.update([i1], |b| b | (m << i2));
        }
      }
      for i1 in (0u64..64).step_by(8) {
        let m: u64 = _mm512_cmpneq_epu64_mask(_mm512_loadu_epi64(
          self.tile_bitmaps.get_ref([i1]) as *const u64 as *const i64
        ), zeroes).into();
        self.tile_bitmaps.compute_root(|b| b | (m << i1));
      }

      eprintln!("Creating microtile data structures");
      for i1 in 0u32..16 {
        for i2 in 0u32..16 {
          for i3 in 0u32..16 {
            for i4 in 0u32..16 {
              for i5 in 0u32..16 {
                let m = _mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3,i4,i5]).into());
                self.microtile_free_maps.get_mut([i1,i2,i3,i4])[usz!(i5)] = (m & !15) | i5;
              }
              let m = _mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3,i4]).into());
              self.microtile_free_maps.get_mut([i1,i2,i3])[usz!(i4)] = (m & !15) | i4;
            }
            let m = _mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3]).into());
            self.microtile_free_maps.get_mut([i1,i2])[usz!(i3)] = (m & !15) | i3;
          }
          let m = _mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2]).into());
          self.microtile_free_maps.get_mut([i1])[usz!(i2)] = (m & !15) | i2;
        }
        let m = _mm512_reduce_max_epu32(self.microtile_free_maps.get([i1]).into());
        self.microtile_free_maps.root_mut()[usz!(i1)] = (m & !15) | i1;
      }
    }

    eprintln!("Loaded freelist");
  }

  fn consume_microtile_space(&mut self, bytes_needed: u32) -> u64 {
    unsafe {
      let y512 = _mm512_set1_epi32(i32::try_from(bytes_needed).unwrap() << 8);

      let m1 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.root().into(), y512);
      let p1 = usz!(_tzcnt_u32(m1.into()));
      // Use u32 for `i{1..6}` as we'll multiply them.
      let (microtile_addr, cur_free, i1, i2, i3, i4, i5, i6) = if p1 == 32 {
        // There are no microtiles, so allocate a new one.
        let microtile_addr = self.fast_allocate_one_tile();
        eprintln!("No existing microtile available, so converting tile {}", microtile_addr);
        let cur_free = TILE_SIZE;
        let mut itmp: u32 = microtile_addr;
        let i6 = itmp % 16; itmp /= 16;
        let i5 = itmp % 16; itmp /= 16;
        let i4 = itmp % 16; itmp /= 16;
        let i3 = itmp % 16; itmp /= 16;
        let i2 = itmp % 16; itmp /= 16;
        let i1 = itmp % 16;
        (microtile_addr, cur_free, i1, i2, i3, i4, i5, i6)
      } else {
        assert!(p1 <= 15, "invalid microtile free map p1 {p1}");
        let i1 = self.microtile_free_maps.root()[p1] & 127;
        assert!(i1 <= 15, "invalid microtile free map i1 {i1}");
        let m2 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.get([i1]).into(), y512);
        let p2 = usz!(_tzcnt_u32(m2.into()));
        assert!(p2 <= 15, "invalid microtile free map p2 {p2}");

        let i2 = self.microtile_free_maps.get([i1])[p2] & 127;
        assert!(i2 <= 15, "invalid microtile free map i2 {i2}");
        let m3 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.get([i1,i2]).into(), y512);
        let p3 = usz!(_tzcnt_u32(m3.into()));
        assert!(p3 <= 15, "invalid microtile free map p3 {p3}");

        let i3 = self.microtile_free_maps.get([i1,i2])[p3] & 127;
        assert!(i3 <= 15, "invalid microtile free map i3 {i3}");
        let m4 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.get([i1,i2,i3]).into(), y512);
        let p4 = usz!(_tzcnt_u32(m4.into()));
        assert!(p4 <= 15, "invalid microtile free map p4 {p4}");

        let i4 = self.microtile_free_maps.get([i1,i2,i3])[p4] & 127;
        assert!(i4 <= 15, "invalid microtile free map i4 {i4}");
        let m5 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.get([i1,i2,i3,i4]).into(), y512);
        let p5 = usz!(_tzcnt_u32(m5.into()));
        assert!(p5 <= 15, "invalid microtile free map p5 {p5}");

        let i5 = self.microtile_free_maps.get([i1,i2,i3,i4])[p5] & 127;
        assert!(i5 <= 15, "invalid microtile free map i5 {i4}");
        let m6 = _mm512_cmpge_epu32_mask(self.microtile_free_maps.get([i1,i2,i3,i4,i5]).into(), y512);
        let p6 = usz!(_tzcnt_u32(m6.into()));
        assert!(p6 <= 15, "invalid microtile free map p6 {p6}");

        let meta: u32 = self.microtile_free_maps.get([i1,i2,i3,i4,i5])[p6];
        let cur_free = u64::from(meta) >> 8;
        let i6 = meta & 127;
        assert!(i6 <= 15, "invalid microtile free map i6 {i6}");
        let microtile_addr = (((((((((i1 * 16) + i2) * 16) + i3) * 16) + i4) * 16) + i5) * 16) + i6;
        eprintln!("Found microtile {} with {} free space", microtile_addr, cur_free);
        (microtile_addr, cur_free, i1, i2, i3, i4, i5, i6)
      };

      // TODO assert cur_free >= bytes_needed.
      let new_free: u32 = u32::try_from(cur_free).unwrap() - bytes_needed;
      eprintln!("Microtile {} now has {} free space", microtile_addr, new_free);
      self.microtile_free_maps.get_mut([i1,i2,i3,i4,i5])[usz!(i6)] = (new_free << 8) | i6;
      self.microtile_free_maps.get_mut([i1,i2,i3,i4])[usz!(i5)] = (_mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3,i4,i5]).into()) & !15) | i5;
      self.microtile_free_maps.get_mut([i1,i2,i3])[usz!(i4)] = (_mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3,i4]).into()) & !15) | i4;
      self.microtile_free_maps.get_mut([i1,i2])[usz!(i3)] = (_mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2,i3]).into()) & !15) | i3;
      self.microtile_free_maps.get_mut([i1])[usz!(i2)] = (_mm512_reduce_max_epu32(self.microtile_free_maps.get([i1,i2]).into()) & !15) | i2;
      self.microtile_free_maps.root_mut()[usz!(i1)] = (_mm512_reduce_max_epu32(self.microtile_free_maps.get([i1]).into()) & !15) | i1;

      u64::from(microtile_addr) * TILE_SIZE + (TILE_SIZE - cur_free)
    }
  }

  fn consume_one_tile(&mut self) -> u32 {
    unsafe { self.fast_allocate_one_tile() }
  }

  fn consume_tiles(&mut self, tiles_needed: u64) -> Vec<u32> {
    assert!(tiles_needed > 0);
    let mut out = Vec::new();
    let mut tiles_remaining = tiles_needed;

    unsafe {
    let i1_candidates = find_indices_of_nonzero_bits_64(self.tile_bitmaps.root());
    'outer: for i1 in i1_candidates {
      let i1: u64 = i1.into();
      let i2_candidates = find_indices_of_nonzero_bits_64(self.tile_bitmaps.get([i1]));
      for i2 in i2_candidates {
        let i2: u64 = i2.into();
        let i3_candidates = find_indices_of_nonzero_bits_64(self.tile_bitmaps.get([i1,i2]));
        for i3 in i3_candidates {
          let i3: u64 = i3.into();
          let mut bitmap = self.tile_bitmaps.get([i1,i2,i3]);
          let result = find_free_tiles_in_region(bitmap, min(tiles_remaining.try_into().unwrap(), 64));
          for &e  in result.as_array() {
            if e == 64 {
              break;
            };
            bitmap &= !(1u64 << e);
            let tile = (((((i1 * 64) + i2) * 64) + i3) * 64) + u64::from(e);
            out.push(tile.try_into().unwrap());
            tiles_remaining -= 1;
          };
          self.propagate_tile_bitmap_change(bitmap, i1, i2, i3);

          if tiles_remaining == 0 {
            break 'outer;
          };
        };
      };
    };
  };

    if tiles_remaining > 0 {
      panic!("failed to allocate {tiles_remaining} of {tiles_needed} requested tiles, most likely out of space");
    };

    out
  }

  fn replenish_tiles(&mut self, tiles: &[u32]) {
    for& tile_no in tiles {
      let mut itmp: u64 = tile_no.into();
      let i4 = itmp % 64; itmp /= 64;
      let i3 = itmp % 64; itmp /= 64;
      let i2 = itmp % 64; itmp /= 64;
      let i1 = itmp % 64;
      self.tile_bitmaps.update([i1,i2,i3], |bm| bm | (1u64 << i4));
    };
  }
}
