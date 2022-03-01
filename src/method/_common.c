#include "../bucket.h"
#include "../cursor.h"
#include "../device.h"
#include "../inode.h"
#include "../log.h"
#include "../server.h"
#include "../tile.h"
#include "_common.h"
#include "../../ext/xxHash/xxhash.h"

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  uint8_t bucket_count_log2,
  method_common_key_t* out
) {
  uint8_t* p = NULL;

  if ((p = svr_method_args_parser_parse(parser, 1)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  if ((out->len = *p) > 128) return METHOD_ERROR_KEY_TOO_LONG;
  if ((p = svr_method_args_parser_parse(parser, out->len)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  memcpy(out->data.bytes, p, out->len);
  memset(out->data.bytes + out->len, 0, 129 - out->len);
  out->bucket = XXH3_64bits(out->data.bytes, out->len) & ((1llu << bucket_count_log2) - 1);

  return METHOD_ERROR_OK;
}

// Returns pointer to start of inode on mmap, or NULL if not found.
cursor_t* method_common_find_inode_in_bucket(
  bucket_t* bkt,
  method_common_key_t* key,
  device_t* dev,
  ino_state_t required_state
) {
  uint32_t bkt_microtile = bkt->microtile;
  uint32_t bkt_microtile_offset = bkt->microtile_byte_offset;
  while (bkt_microtile) {
    cursor_t* cur = dev->mmap + (TILE_SIZE * bkt_microtile) + bkt_microtile_offset;
    uint64_t checksum_recorded = read_u64(cur);
    uint32_t inode_size_checksummed = read_u24(cur + 8);
    uint64_t checksum_actual = XXH3_64bits(cur + 8, inode_size_checksummed);
    if (checksum_recorded != checksum_actual) {
      CORRUPT("inode at tile %u offset %u has recorded checksum %x but data checksums to %x", bkt_microtile, bkt_microtile_offset, checksum_recorded, checksum_actual);
    }
    bkt_microtile = read_u24(cur + 8 + 3);
    bkt_microtile_offset = read_u24(cur + 8 + 3 + 3);
    ino_state_t ino_state = cur[8 + 3 + 3 + 3];
    if (ino_state != required_state) {
      continue;
    }
    uint8_t ino_key_len = cur[8 + 3 + 3 + 3 + 1 + 5];
    if (ino_key_len != key->len) {
      continue;
    }
    uint8_t ino_key[64];
    memcpy(ino_key, cur + 8 + 3 + 3 + 3 + 1 + 5 + 1, min(ino_key_len, 64));
    if (ino_key_len < 64) {
      memset(ino_key, 0, 64 - ino_key_len);
    }
    __m512i ino_key_lower = _mm512_loadu_epi8(ino_key);
    // WARNING: Both __m512i arguments must be filled with the same character.
    if (_mm512_cmpneq_epi8_mask(ino_key_lower, key->data.vecs[0])) {
      continue;
    }
    if (ino_key_len > 64) {
      memcpy(ino_key, cur + 8 + 3 + 3 + 3 + 1 + 5 + 1, ino_key_len - 64);
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
