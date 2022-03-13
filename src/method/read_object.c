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
#include "../server_method_args.h"
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "read_object.h"

LOGGER("method_create_object");

// Method signature: (u8 key_len, char[] key, i64 start, i64 end_exclusive_or_zero_for_eof).
// Requested range cannot be empty.
void method_read_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_read_object_state_t* args = (method_read_object_state_t*) state_raw;

  uint8_t* p = NULL;
  args->read_count = 0;
  args->obj_no = 0;
  INIT_STATE_RESPONSE(args, METHOD_READ_OBJECT_RESPONSE_LEN);

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->arg_start = read_i64(p);

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->arg_end = read_i64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("read_object(key=%s, start=%ld, end=%ld)", args->key.data.bytes, args->arg_start, args->arg_end);
}

svr_client_result_t method_read_object(
  method_ctx_t* ctx,
  void* args_raw,
  svr_client_t* client
) {
  method_read_object_state_t* args = (method_read_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, METHOD_READ_OBJECT_RESPONSE_LEN, client, args->response[0] != METHOD_ERROR_OK);

  svr_client_result_t res;
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&ctx->bkts->bucket_locks[args->key.bucket]), "lock bucket");

  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &args->key,
    INO_STATE_READY,
    args->obj_no,
    NULL
  );

  if (!inode_dev_offset) {
    if (args->response_written == -1) {
      ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
    }
    // The object has been deleted while we were reading it.
    // TODO Should we use some framing protocol or format so we don't have to close the connection?
    ts_log(DEBUG, "Connection attempting to read from now-deleted object with key %s will be disconnected", args->key.data.bytes);
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;
  if (!args->obj_no) {
    args->obj_no = read_u64(inode_cur + INO_OFFSETOF_OBJ_NO);
    int64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
    uint64_t actual_start;
    uint64_t actual_end_excl;
    if (args->arg_start < 0) {
      if (-args->arg_start >= size) {
        ERROR_RESPONSE(METHOD_ERROR_INVALID_START);
      }
      actual_start = size + args->arg_start;
    } else {
      if (args->arg_start >= size) {
        ERROR_RESPONSE(METHOD_ERROR_INVALID_START);
      }
      actual_start = args->arg_start;
    }
    if (args->arg_end < 0) {
      if (-args->arg_end >= size) {
        ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
      }
      actual_end_excl = size + args->arg_end;
    } else if (args->arg_end == 0) {
      actual_end_excl = size;
    } else {
      if (args->arg_end >= size) {
        ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
      }
      actual_end_excl = args->arg_end;
    }
    // Range cannot be empty.
    if (actual_end_excl <= actual_start) {
      ERROR_RESPONSE(METHOD_ERROR_INVALID_END);
    }
    args->actual_start = actual_start;
    args->actual_length = actual_end_excl - actual_start;
    args->object_size = size;
    args->response_written = 0;
    args->response[0] = METHOD_ERROR_OK;
    write_u64(args->response + 1, actual_start);
    write_u64(args->response + 9, args->actual_length);
    write_u64(args->response + 17, args->object_size);
    res = SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
    goto final;
  }

  uint8_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];

  uint64_t read_start = args->actual_start + args->read_count;
  uint64_t tile_no = read_start / TILE_SIZE;
  uint64_t read_part_offset = read_start % TILE_SIZE;
  uint64_t full_tile_count = args->object_size / TILE_SIZE;
  cursor_t* read_offset;
  uint64_t read_part_max_len;
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    // We're reading from the last tile.
    read_offset = inode_cur + INO_OFFSETOF_LAST_TILE_INLINE_DATA(args->key.len, full_tile_count);
    read_part_max_len = (args->object_size % TILE_SIZE) - read_part_offset;
  } else {
    uint32_t tile_addr = read_u24(inode_cur + INO_OFFSETOF_TILE_NO(args->key.len, tile_no));
    read_offset = ctx->dev->mmap + (tile_addr * TILE_SIZE);
    read_part_max_len = TILE_SIZE - read_part_offset;
  }

  int writeno = maybe_write(client->fd, read_offset, read_part_max_len);
  if (-1 == writeno) {
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  args->read_count += writeno;
  // TODO Assert not greater than.
  if (args->read_count == args->actual_length) {
    res = SVR_CLIENT_RESULT_END;
    goto final;
  }
  res = SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
  goto final;

  final:
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->bkts->bucket_locks[args->key.bucket]), "unlock bucket");
  return res;
}
