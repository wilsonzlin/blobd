#define _GNU_SOURCE

#include "_common.h"
#include "../../ext/coz/include/coz.h"
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

#ifdef TURBOSTORE_INODE_KEY_CMP_AVX512
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, method_common_key_data_t b, uint8_t b_len) {
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
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, method_common_key_data_t b, uint8_t b_len) {
  return a_len == b_len && 0 == memcmp(a, b.bytes, a_len);
}
#endif

#ifdef TURBOSTORE_DEBUG_LOG_LOOKUPS
#define DEBUG_TS_LOG_LOOKUP(fmt, ...) DEBUG_TS_LOG(fmt, ##__VA_ARGS__)
#else
#define DEBUG_TS_LOG_LOOKUP(fmt, ...) ((void) 0)
#endif

// Returns NULL if not found.
// WARNING: Run `atomic_fetch_sub_explicit(&inode->refcount, 1, memory_order_relaxed)` after completing read/write/inspect/etc.
inode_t* method_common_find_inode_in_bucket_for_non_management(
  bucket_t* bkt,
  method_common_key_t* key,
  device_t* dev,
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero
) {
  COZ_BEGIN("method_common_find_inode_in_bucket_for_non_management");
  // WARNING: There are very subtle behaviours here that prevent race conditions:
  // - Incrementing the refcount must be done BEFORE looking up the state. If we don't, it's possible for the inode and tile spaces to be freed and overwritten with other data, before we read the state and other fields or object data in tiles.
  // - The inode_t must always point to a valid address (e.g. using a pool). If we allocated it using malloc(), it's possible that we get to the value before the previous "next" is updated, but it gets free()'d before we manage to increment the refcount. If it's a pool value, it's possible the value has changed, but it's still safe to read the fields and detect that it's changed.
  DEBUG_TS_LOG_LOOKUP("Trying to find %s with state %d and object number %lu", key->data.bytes, allowed_states, required_obj_no_or_zero);
  inode_t* found = NULL;
  for (
    inode_t* bkt_ino = atomic_load_explicit(&bkt->head, memory_order_relaxed);
    bkt_ino != NULL;
    bkt_ino = atomic_load_explicit(&bkt_ino->next, memory_order_relaxed)
  ) {
    cursor_t* cur = INODE_CUR(dev, bkt_ino);
    DEBUG_TS_LOG_LOOKUP("Looking at inode with object number %lu, state %d, and key %s", read_u64(cur + INO_OFFSETOF_OBJ_NO), atomic_load_explicit(&bkt_ino->state, memory_order_relaxed), cur + INO_OFFSETOF_KEY);
    atomic_fetch_add_explicit(&bkt_ino->refcount, 1, memory_order_relaxed);
    DEBUG_ASSERT_STATE(INODE_STATE_IS_VALID(cur[INO_OFFSETOF_STATE]), "inode at device offset %lu does not have a valid state (%u)", INODE_DEV_OFFSET(bkt_ino), cur[INO_OFFSETOF_STATE]);
    DEBUG_ASSERT_STATE(cur[INO_OFFSETOF_KEY_NULL_TERM(cur[INO_OFFSETOF_KEY_LEN])] == 0, "inode at device offset %lu does not have key null terminator", INODE_DEV_OFFSET(bkt_ino));
    if (
      // Use memory_order_acquire to ensure all inode field values read from mmap are latest.
      (atomic_load_explicit(&bkt_ino->state, memory_order_acquire) & allowed_states) &&
      (required_obj_no_or_zero == 0 || read_u64(cur + INO_OFFSETOF_OBJ_NO) == required_obj_no_or_zero) &&
      cur[INO_OFFSETOF_KEY_LEN] == key->len &&
      compare_raw_key_with_vec_key(cur + INO_OFFSETOF_KEY, cur[INO_OFFSETOF_KEY_LEN], key->data, key->len)
    ) {
      // Do NOT decrement refcount if it's the inode we want; we must decrement only once we're done.
      found = bkt_ino;
      break;
    }
    atomic_fetch_sub_explicit(&bkt_ino->refcount, 1, memory_order_relaxed);
  }
  if (found == NULL) DEBUG_TS_LOG_LOOKUP("%s not found", key->data.bytes);
  COZ_END("method_common_find_inode_in_bucket_for_non_management");
  return found;
}
