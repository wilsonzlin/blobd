#pragma once

#include <errno.h>
#include <immintrin.h>
#include <stdint.h>
#include "../bucket.h"
#include "../cursor.h"
#include "../device.h"
#include "../inode.h"
#include "../server_method_args.h"
#include "../util.h"

typedef enum {
  // Dummy value.
  SVR_METHOD__UNKNOWN = 0,
  SVR_METHOD_CREATE_OBJECT = 1,
  SVR_METHOD_INSPECT_OBJECT = 2,
  SVR_METHOD_READ_OBJECT = 3,
  SVR_METHOD_WRITE_OBJECT = 4,
  SVR_METHOD_COMMIT_OBJECT = 5,
  SVR_METHOD_DELETE_OBJECT = 6,
} method_t;

typedef enum {
  METHOD_ERROR_OK = 0,
  METHOD_ERROR_NOT_ENOUGH_ARGS = 1,
  METHOD_ERROR_KEY_TOO_LONG = 2,
  METHOD_ERROR_TOO_MANY_ARGS = 3,
  METHOD_ERROR_NOT_FOUND = 4,
  METHOD_ERROR_INVALID_START = 5,
  METHOD_ERROR_INVALID_END = 6,
} method_error_t;

#define INIT_STATE_RESPONSE(state, response_len) \
  memset(state->response, 0, response_len); \
  state->response_written = -1

#define PARSE_ERROR(state, error) \
  state->response_written = 0; \
  state->response[0] = error; \
  return state;

#define ERROR_RESPONSE(error) \
  args->response[0] = error; \
  args->response_written = 0; \
  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;

#define MAYBE_HANDLE_RESPONSE(state, response_len, client_fd, return_on_end) \
  if (state->response_written >= 0 && state->response_written < response_len) { \
    int maybehandleresponsewriteresult = maybe_write(client_fd, state->response + state->response_written, response_len - state->response_written); \
    if (-1 == maybehandleresponsewriteresult) { \
      return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
    } \
    if (0 == maybehandleresponsewriteresult) { \
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
    } \
    if ((state->response_written += maybehandleresponsewriteresult) < RESPONSE_LEN) { \
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
    } \
    if (return_on_end) { \
      return SVR_CLIENT_RESULT_END; \
    } \
  }

typedef union {
  __m512i vecs[2];
  uint8_t bytes[129];
} method_common_key_data_t;

typedef struct {
  method_common_key_data_t data __attribute__((aligned (64)));
  uint8_t len;
  uint64_t bucket;
} method_common_key_t;

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  buckets_t* bkts,
  method_common_key_t* out
);

inode_t* method_common_find_inode_in_bucket_for_non_management(
  bucket_t* bkt,
  method_common_key_t* key,
  device_t* dev,
  // Bitwise OR of all allowed states.
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero
);

#define INODE_CUR(dev, bkt_ino) ((dev)->mmap + ((bkt_ino)->tile * TILE_SIZE) + (bkt_ino)->tile_offset)

#define METHOD_COMMON_ITERATE_INODES_IN_BUCKET_FOR_MANAGEMENT(bkt, key, dev, allowed_states, required_obj_no_or_zero, bkt_ino, prev_expr) \
  for ( \
    inode_t* bkt_ino = atomic_load_explicit(&(bkt)->head, memory_order_relaxed); \
    bkt_ino != NULL; \
    (prev_expr), bkt_ino = atomic_load_explicit(&bkt_ino->next, memory_order_relaxed) \
  ) \
    /* We don't need to increment refcount, as we're in the single-threaded manager. */ \
    if ( \
      (atomic_load_explicit(&bkt_ino->state, memory_order_relaxed) & (allowed_states)) && \
      ((required_obj_no_or_zero) == 0 || read_u64(INODE_CUR(dev, bkt_ino) + INO_OFFSETOF_OBJ_NO) == (required_obj_no_or_zero)) && \
      INODE_CUR(dev, bkt_ino)[INO_OFFSETOF_KEY_LEN] == (key)->len && \
      compare_raw_key_with_vec_key(INODE_CUR(dev, bkt_ino) + INO_OFFSETOF_KEY, INODE_CUR(dev, bkt_ino)[INO_OFFSETOF_KEY_LEN], (key)->data.vecs[0], (key)->data.vecs[1]) \
    )
