#define _GNU_SOURCE

#include <errno.h>
#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../cursor.h"
#include "../device.h"
#include "../exit.h"
#include "../inode.h"
#include "../log.h"
#include "../object.h"
#include "../server.h"
#include "../tile.h"
#include "../util.h"
#include "_common.h"
#include "read_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_create_object");

// [u8 error, u64 actual_read_start, u64 actual_read_len].
#define RESPONSE_LEN (1 + 8 + 8)

struct method_read_object_state_s {
  uint32_t read_count;
  // 0 if not found yet.
  uint64_t obj_no;
  // -1 if not prepared yet.
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  int64_t arg_start;
  int64_t arg_end;
  // These are undefiend if not found yet.
  uint64_t actual_start;
  uint64_t actual_length;
  uint64_t object_size;
};

// Method signature: (u8 key_len, char[] key, i64 start, i64 end_exclusive_or_zero_for_eof).
// Requested range cannot be empty.
method_read_object_state_t* method_read_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_read_object_state_t* args = aligned_alloc(64, sizeof(method_read_object_state_t));
  uint8_t* p = NULL;
  args->read_count = 0;
  args->obj_no = 0;
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts->count_log2, &args->key);
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

  return args;
}

void method_read_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_read_object(
  svr_method_handler_ctx_t* ctx,
  method_read_object_state_t* args,
  int client_fd
) {
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, false);

  ts_log(DEBUG, "read_object(key=%s)", args->key.data.bytes);

  bool acquired_lock = false;
  svr_client_result_t res;
  // We must look up again each time in case it has been deleted since we last held the lock.
  // This may seem inefficient but it's better than holding the lock the entire time.
  bucket_t* bkt = &ctx->bkts->buckets[args->key.bucket];
  if (pthread_rwlock_rdlock(&bkt->lock)) {
    perror("Failed to acquire read lock on bucket");
    exit(EXIT_INTERNAL);
  }
  acquired_lock = true;

  cursor_t* inode_cur = method_common_find_inode_in_bucket(bkt, &args->key, ctx->dev, INO_STATE_READY, args->obj_no);
  if (inode_cur == NULL) {
    if (args->response_written == -1) {
      ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
    }
    // The object has been deleted while we were reading it.
    res = SVR_CLIENT_RESULT_END;
    goto final;
  }
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

  int writeno = maybe_write(client_fd, read_offset, read_part_max_len);
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

  final:
  if (acquired_lock && pthread_rwlock_unlock(&bkt->lock)) {
    perror("Failed to release read lock on bucket");
    exit(EXIT_INTERNAL);
  }

  return res;
}
