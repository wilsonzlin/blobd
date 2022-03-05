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

// Returns NULL if not found.
// WARNING: Run `atomic_fetch_sub_explicit(&inode->refcount, 1, memory_order_relaxed)` after completing read/write/inspect/etc.
inode_t* method_common_find_inode_in_bucket_for_non_management(
  bucket_t* bkt,
  method_common_key_t* key,
  device_t* dev,
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero
) {
  // WARNING: There are very subtle behaviours here that prevent race conditions:
  // - Incrementing the refcount must be done BEFORE looking up the state. If we don't, it's possible for the inode and tile spaces to be freed and overwritten with other data, before we read the state and other fields.
  // - The inode_t must always point to a valid address (e.g. using a pool). If we allocated it using malloc(), it's possible that we get to the value before the previous "next" is updated, but it gets free()'d before we manage to increment the refcount. If it's a pool value, it's possible the value has changed, but it's still safe to read the fields and detect that it's changed.
  for (
    inode_t* bkt_ino = atomic_load_explicit(&bkt->head, memory_order_relaxed);
    bkt_ino != NULL;
    bkt_ino = atomic_load_explicit(&bkt_ino->next, memory_order_relaxed)
  ) {
    cursor_t* cur = dev->mmap + (bkt_ino->tile * TILE_SIZE) + bkt_ino->tile_offset;
    atomic_fetch_add_explicit(&bkt_ino->refcount, 1, memory_order_relaxed);
    if (
      (atomic_load_explicit(&bkt_ino->state, memory_order_relaxed) & allowed_states) &&
      (required_obj_no_or_zero == 0 || read_u64(cur + INO_OFFSETOF_OBJ_NO) == required_obj_no_or_zero) &&
      cur[INO_OFFSETOF_KEY_LEN] == key->len &&
      compare_raw_key_with_vec_key(cur + INO_OFFSETOF_KEY, cur[INO_OFFSETOF_KEY_LEN], key->data.vecs[0], key->data.vecs[1])
    ) {
      // Do NOT decrement refcount if it's the inode we want; we must decrement only once we're done.
      return bkt_ino;
    }
    atomic_fetch_sub_explicit(&bkt_ino->refcount, 1, memory_order_relaxed);
  }
  return NULL;
}

#define INODE_CUR(dev, bkt_ino) (dev->mmap + (bkt_ino->tile * TILE_SIZE) + bkt_ino->tile_offset)

#define METHOD_COMMON_ITERATE_INODES_IN_BUCKET_FOR_MANAGEMENT(bkt, key, dev, allowed_states, required_obj_no_or_zero, bkt_ino, should_output_prev, out_prev_ino_or_null) \
  for ( \
    inode_t* bkt_ino = atomic_load_explicit(&bkt->head, memory_order_relaxed); \
    bkt_ino != NULL; \
    (should_output_prev ? (out_prev_ino_or_null = bkt_ino) : 0), bkt_ino = atomic_load_explicit(&bkt_ino->next, memory_order_relaxed) \
  ) \
    /* We don't need to increment refcount, as we're in the single-threaded manager. */ \
    if ( \
      (atomic_load_explicit(&bkt_ino->state, memory_order_relaxed) & allowed_states) && \
      (required_obj_no_or_zero == 0 || read_u64(INODE_CUR(dev, bkt_ino) + INO_OFFSETOF_OBJ_NO) == required_obj_no_or_zero) && \
      INODE_CUR(dev, bkt_ino)[INO_OFFSETOF_KEY_LEN] == key->len && \
      compare_raw_key_with_vec_key(INODE_CUR(dev, bkt_ino) + INO_OFFSETOF_KEY, INODE_CUR(dev, bkt_ino)[INO_OFFSETOF_KEY_LEN], key->data.vecs[0], key->data.vecs[1]) \
    )
