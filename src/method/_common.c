#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "_common.h"
#include "../bucket.h"
#include "../cursor.h"
#include "../device.h"
#include "../inode.h"
#include "../log.h"
#include "../server.h"
#include "../tile.h"
#include "../util.h"

LOGGER("method_common");

method_error_t method_common_key_parse(
  uint8_t** args_raw,
  buckets_t* bkts,
  method_common_key_t* out
) {
  if ((out->len = **args_raw) > 128) return METHOD_ERROR_KEY_TOO_LONG;
  *args_raw += 1;
  memcpy(out->data.bytes, *args_raw, out->len);
  *args_raw += out->len;
  memset(out->data.bytes + out->len, 0, 129 - out->len);
  out->bucket = BUCKET_ID_FOR_KEY(out->data.bytes, out->len, bkts->key_mask);

  return METHOD_ERROR_OK;
}

#ifdef TURBOSTORE_INODE_KEY_CMP_AVX512
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, method_common_key_data_t b) {
  __m512i b_lower = b.vecs[0];
  __m512i b_upper = b.vecs[0];
  vec_512i_u8_t ino_key;
  memcpy(ino_key.elems, a, min(a_len, 64));
  if (a_len < 64) {
    memset(ino_key.elems + a_len, 0, 64 - a_len);
  }
  // WARNING: Both __m512i arguments must be filled with the same character.
  if (_mm512_cmpneq_epi8_mask(ino_key.vec, b_lower)) {
    return false;
  }
  if (a_len > 64) {
    memcpy(ino_key.elems, a + 64, a_len - 64);
    memset(ino_key.elems + (a_len - 64), 0, 128 - (a_len - 64));
    // WARNING: Both __m512i arguments must be filled with the same character.
    if (_mm512_cmpneq_epi8_mask(ino_key.vec, b_upper)) {
      return false;
    }
  }
  return true;
}
#else
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, method_common_key_data_t b) {
  return 0 == memcmp(a, b.bytes, a_len);
}
#endif

#ifdef TURBOSTORE_DEBUG_LOG_LOOKUPS
#define DEBUG_TS_LOG_LOOKUP(fmt, ...) DEBUG_TS_LOG(fmt, ##__VA_ARGS__)
#else
#define DEBUG_TS_LOG_LOOKUP(fmt, ...) ((void) 0)
#endif

// Returns 0 if not found.
uint64_t method_common_find_inode_in_bucket(
  device_t* dev,
  buckets_t* buckets,
  method_common_key_t* key,
  // Bitwise OR of all allowed states.
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero,
  // Will not be updated if returned inode is head, or if 0 is returned (i.e. not found).
  uint64_t* out_prev_inode_dev_offset_or_null
) {
  uint64_t dev_offset = read_u48(dev->mmap + buckets->dev_offset + BUCKETS_OFFSETOF_BUCKET(key->bucket));
  DEBUG_TS_LOG_LOOKUP("Trying to find %s with state %d and object number %lu", key->data.bytes, allowed_states, required_obj_no_or_zero);
  while (dev_offset) {
    cursor_t* cur = dev->mmap + dev_offset;
    DEBUG_TS_LOG_LOOKUP("Looking at inode with object number %lu, state %d, and key %s", read_u64(cur + INO_OFFSETOF_OBJ_NO), cur[INO_OFFSETOF_STATE], cur + INO_OFFSETOF_KEY);
    DEBUG_ASSERT_STATE(INODE_STATE_IS_VALID(cur[INO_OFFSETOF_STATE]), "inode at device offset %lu does not have a valid state (%u)", dev_offset, cur[INO_OFFSETOF_STATE]);
    DEBUG_ASSERT_STATE(cur[INO_OFFSETOF_KEY_NULL_TERM(cur[INO_OFFSETOF_KEY_LEN])] == 0, "inode at device offset %lu does not have key null terminator", dev_offset);
    if (
      (cur[INO_OFFSETOF_STATE] & allowed_states) &&
      (required_obj_no_or_zero == 0 || read_u64(cur + INO_OFFSETOF_OBJ_NO) == required_obj_no_or_zero) &&
      cur[INO_OFFSETOF_KEY_LEN] == key->len &&
      compare_raw_key_with_vec_key(cur + INO_OFFSETOF_KEY, cur[INO_OFFSETOF_KEY_LEN], key->data)
    ) {
      return dev_offset;
    }
    if (out_prev_inode_dev_offset_or_null != NULL) *out_prev_inode_dev_offset_or_null = dev_offset;
  }
  DEBUG_TS_LOG_LOOKUP("%s not found", key->data.bytes);
  return 0;
}
