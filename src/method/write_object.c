#define _GNU_SOURCE

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
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
#include "write_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_write_object");

#define RESPONSE_LEN 1

struct method_write_object_state_s {
  uint32_t written;
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  uint64_t obj_no;
  uint64_t start;
};

// Method signature: (u8 key_len, char[] key, u64 obj_no, u64 start).
// Only INCOMPLETE objects can be written to. Objects can only be written in TILE_SIZE chunks, except for the last. Each chunk must start at a multiple of TILE_SIZE.
method_write_object_state_t* method_write_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_write_object_state_t* args = aligned_alloc(64, sizeof(method_write_object_state_t));
  args->written = 0;
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, RESPONSE_LEN);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->obj_no = read_u64(p);

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->start = read_u64(p);
  if ((args->start % TILE_SIZE) != 0) {
    PARSE_ERROR(args, METHOD_ERROR_INVALID_START);
  }

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  return args;
}

void method_write_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_write_object(
  svr_method_handler_ctx_t* ctx,
  method_write_object_state_t* args,
  int client_fd
) {
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, true);

  ts_log(DEBUG, "write_object(key=%s, start=%ld)", args->key.data.bytes, args->start);

  bool acquired_lock = false;
  svr_client_result_t res;
  // We must look up again each time in case it has been deleted since we last held the lock.
  // This may seem inefficient but it's better than holding the lock the entire time.
  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);
  if (pthread_rwlock_rdlock(&bkt->lock)) {
    perror("Failed to acquire read lock on bucket");
    exit(EXIT_INTERNAL);
  }
  acquired_lock = true;

  cursor_t* inode_cur = method_common_find_inode_in_bucket(bkt, &args->key, ctx->dev, INO_STATE_INCOMPLETE, args->obj_no);

  if (inode_cur == NULL) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }

  uint64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
  if (args->start >= size) {
    ERROR_RESPONSE(METHOD_ERROR_INVALID_START);
  }

  uint8_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];

  uint64_t tile_no = args->start / TILE_SIZE;
  uint64_t full_tile_count = size / TILE_SIZE;
  cursor_t* write_offset;
  uint64_t write_max_len;
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    // We're writing to the last tile.
    write_offset = inode_cur + INO_OFFSETOF_LAST_TILE_INLINE_DATA(args->key.len, full_tile_count);
    write_max_len = size % TILE_SIZE;
  } else {
    uint32_t tile_addr = read_u24(inode_cur + INO_OFFSETOF_TILE_NO(args->key.len, tile_no));
    write_offset = ctx->dev->mmap + (tile_addr * TILE_SIZE);
    write_max_len = TILE_SIZE;
  }

  // TODO Assert not greater than.
  if (args->written == write_max_len) {
    ERROR_RESPONSE(METHOD_ERROR_OK);
  }

  int readno = maybe_read(client_fd, write_offset, write_max_len - args->written);
  if (-1 == readno) {
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  args->written += readno;
  // TODO Assert not greater than.
  if (args->written == write_max_len) {
    ERROR_RESPONSE(METHOD_ERROR_OK);
  }
  res = SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;

  final:
  if (acquired_lock && pthread_rwlock_unlock(&bkt->lock)) {
    perror("Failed to release read lock on bucket");
    exit(EXIT_INTERNAL);
  }

  return res;
}
