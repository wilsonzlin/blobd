#include <errno.h>
#include <immintrin.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "_common.h"
#include "../cursor.h"
#include "../device.h"
#include "../exit.h"
#include "../inode.h"
#include "../log.h"
#include "../object.h"
#include "../server_client.h"
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "read_object.h"

LOGGER("method_create_object");

method_error_t method_read_object_parse(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  uint8_t* args_cur
) {
  state->read_count = 0;
  state->obj_no = 0;

  method_error_t key_parse_error = method_common_key_parse(&args_cur, ctx->bkts, &state->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    return key_parse_error;
  }

  state->arg_start = consume_i64(&args_cur);
  state->arg_end = consume_i64(&args_cur);

  DEBUG_TS_LOG("read_object(key=%s, start=%ld, end=%ld)", state->key.data.bytes, state->arg_start, state->arg_end);

  return METHOD_ERROR_OK;
}

#define ERROR_RESPONSE(err) \
  state->valid = false; \
  produce_u8(&out_response, err); \
  goto final

svr_client_result_t method_read_object_response(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  (void) client;

  svr_client_result_t res = SVR_CLIENT_RESULT_WRITE_RESPONSE;
  BUCKET_LOCK_READ(ctx->bkts, state->key.bucket);

  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &state->key,
    INO_STATE_READY,
    0,
    NULL
  );

  if (!inode_dev_offset) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }

  cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;
  state->obj_no = read_u64(inode_cur + INO_OFFSETOF_OBJ_NO);
  int64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
  uint64_t actual_start;
  uint64_t actual_end_excl;
  if (state->arg_start < 0) {
    if (-state->arg_start >= size) {
      ERROR_RESPONSE(METHOD_ERROR_INVALID_START);
    }
    actual_start = size + state->arg_start;
  } else {
    if (state->arg_start >= size) {
      ERROR_RESPONSE(METHOD_ERROR_INVALID_START);
    }
    actual_start = state->arg_start;
  }
  if (state->arg_end < 0) {
    if (-state->arg_end >= size) {
      ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
    }
    actual_end_excl = size + state->arg_end;
  } else if (state->arg_end == 0) {
    actual_end_excl = size;
  } else {
    if (state->arg_end >= size) {
      ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
    }
    actual_end_excl = state->arg_end;
  }
  // Range cannot be empty.
  if (actual_end_excl <= actual_start) {
    ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
  }
  state->actual_start = actual_start;
  state->actual_length = actual_end_excl - actual_start;
  state->object_size = size;
  produce_u8(&out_response, METHOD_ERROR_OK);
  produce_u64(&out_response, actual_start);
  produce_u64(&out_response, state->actual_length);
  produce_u64(&out_response, state->object_size);
  goto final;

  final:
  BUCKET_UNLOCK(ctx->bkts, state->key.bucket);
  return res;
}

svr_client_result_t method_read_object_postresponse(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  svr_client_t* client
) {
  if (!state->valid) {
    return SVR_CLIENT_RESULT_END;
  }

  svr_client_result_t res;
  BUCKET_LOCK_READ(ctx->bkts, state->key.bucket);

  // Check that object still exists.
  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &state->key,
    INO_STATE_READY,
    state->obj_no,
    NULL
  );

  if (!inode_dev_offset) {
    // The object has been deleted while we were reading it.
    ts_log(DEBUG, "Connection attempting to read from now-deleted object with key %s will be disconnected", state->key.data.bytes);
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }

  cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;

  uint8_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];

  uint64_t read_start = state->actual_start + state->read_count;
  uint64_t tile_no = read_start / TILE_SIZE;
  uint64_t read_part_offset = read_start % TILE_SIZE;
  uint64_t full_tile_count = state->object_size / TILE_SIZE;
  cursor_t* read_offset;
  uint64_t read_part_max_len;
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    // We're reading from the last tile.
    read_offset = inode_cur + INO_OFFSETOF_LAST_TILE_INLINE_DATA(state->key.len, full_tile_count);
    read_part_max_len = (state->object_size % TILE_SIZE) - read_part_offset;
  } else {
    uint32_t tile_addr = read_u24(inode_cur + INO_OFFSETOF_TILE_NO(state->key.len, tile_no));
    read_offset = ctx->dev->mmap + (tile_addr * TILE_SIZE);
    read_part_max_len = TILE_SIZE - read_part_offset;
  }

  int writeno = maybe_write(client->fd, read_offset, read_part_max_len);
  if (-1 == writeno) {
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  state->read_count += writeno;
  // TODO Assert not greater than.
  if (state->read_count == state->actual_length) {
    res = SVR_CLIENT_RESULT_END;
    goto final;
  }
  res = SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
  goto final;

  final:
  BUCKET_UNLOCK(ctx->bkts, state->key.bucket);
  return res;
}
