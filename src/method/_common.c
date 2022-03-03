#define _GNU_SOURCE

#include "../bucket.h"
#include "../cursor.h"
#include "../device.h"
#include "../inode.h"
#include "../log.h"
#include "../server.h"
#include "../tile.h"
#include "../util.h"
#include "_common.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_common");

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  buckets_t* bkts,
  method_common_key_t* out
) {
  uint8_t* p = NULL;

  if ((p = svr_method_args_parser_parse(parser, 1)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  if ((out->len = *p) > 128) return METHOD_ERROR_KEY_TOO_LONG;
  if ((p = svr_method_args_parser_parse(parser, out->len)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  memcpy(out->data.bytes, p, out->len);
  memset(out->data.bytes + out->len, 0, 129 - out->len);
  out->bucket = buckets_get_bucket_id_for_key(bkts, out->data.bytes, out->len);

  return METHOD_ERROR_OK;
}

// Returns pointer to start of inode on mmap, or NULL if not found.
cursor_t* method_common_find_inode_in_bucket(
  bucket_t* bkt,
  method_common_key_t* key,
  device_t* dev,
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero
) {
  uint32_t bkt_tile = bkt->tile;
  uint32_t bkt_tile_offset = bkt->tile_offset;
  while (bkt_tile) {
    cursor_t* cur = dev->mmap + (TILE_SIZE * bkt_tile) + bkt_tile_offset;
    uint64_t checksum_recorded = read_u64(cur);
    uint32_t inode_size_checksummed = read_u24(cur + 8);
    uint64_t checksum_actual = XXH3_64bits(cur + 8, inode_size_checksummed);
    if (checksum_recorded != checksum_actual) {
      CORRUPT("inode at tile %u offset %u has recorded checksum %x but data checksums to %x", bkt_tile, bkt_tile_offset, checksum_recorded, checksum_actual);
    }
    bkt_tile = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE);
    bkt_tile_offset = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET);
    ino_state_t ino_state = cur[INO_OFFSETOF_STATE];
    if (!(ino_state & allowed_states)) {
      continue;
    }
    if (required_obj_no_or_zero) {
      uint64_t ino_obj_no = read_u64(cur + INO_OFFSETOF_OBJ_NO);
      if (ino_obj_no != required_obj_no_or_zero) {
        continue;
      }
    }
    uint8_t ino_key_len = cur[INO_OFFSETOF_KEY_LEN];
    if (ino_key_len != key->len) {
      continue;
    }
    uint8_t ino_key[64];
    memcpy(ino_key, cur + INO_OFFSETOF_KEY, min(ino_key_len, 64));
    if (ino_key_len < 64) {
      memset(ino_key, 0, 64 - ino_key_len);
    }
    __m512i ino_key_lower = _mm512_loadu_epi8(ino_key);
    // WARNING: Both __m512i arguments must be filled with the same character.
    if (_mm512_cmpneq_epi8_mask(ino_key_lower, key->data.vecs[0])) {
      continue;
    }
    if (ino_key_len > 64) {
      memcpy(ino_key, cur + INO_OFFSETOF_KEY + 64, ino_key_len - 64);
      memset(ino_key, 0, 128 - (ino_key_len - 64));
      __m512i ino_key_upper = _mm512_loadu_epi8(ino_key);
      // WARNING: Both __m512i arguments must be filled with the same character.
      if (_mm512_cmpneq_epi8_mask(ino_key_upper, key->data.vecs[1])) {
        continue;
      }
    }
    return cur;
  }
  return NULL;
}
