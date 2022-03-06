#define _GNU_SOURCE

#include <errno.h>
#include <pthread.h>
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
#include "write_object.h"

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
void* method_write_object_state_create(
  void* ctx_raw,
  svr_method_args_parser_t* parser
) {
  worker_method_handler_ctx_t* ctx = (worker_method_handler_ctx_t*) ctx_raw;

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

  DEBUG_TS_LOG("write_object(key=%s, obj_no=%lu, start=%ld)", args->key.data.bytes, args->obj_no, args->start);

  return args;
}

void method_write_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_write_object(
  void* ctx_raw,
  void* args_raw,
  int client_fd
) {
  worker_method_handler_ctx_t* ctx = (worker_method_handler_ctx_t*) ctx_raw;
  method_write_object_state_t* args = (method_write_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, true);

  svr_client_result_t res;
  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);

  inode_t* found = method_common_find_inode_in_bucket_for_non_management(
    bkt,
    &args->key,
    ctx->dev,
    INO_STATE_INCOMPLETE,
    args->obj_no
  );

  if (found == NULL) {
    // We close the connection in case the client has already written data, and ending without disconnecting would cause the queued data to be interpreted as the start of the next request.
    // TODO Should we use some framing protocol or format so we don't have to close the connection?
    ts_log(DEBUG, "Connection attempting to write to non-existent object with key %s will be disconnected", args->key.data.bytes);
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  uint64_t inode_dev_offset = (TILE_SIZE * found->tile) + found->tile_offset;
  cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;
  uint64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
  if (args->start >= size) {
    // We close the connection in case the client has already written data, and ending without disconnecting would cause the queued data to be interpreted as the start of the next request.
    // TODO Should we use some framing protocol or format so we don't have to close the connection?
    ts_log(DEBUG, "Connection attempting to write past end of object with key %s will be disconnected", args->key.data.bytes);
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }

  uint8_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];

  uint64_t tile_no = args->start / TILE_SIZE;
  uint64_t full_tile_count = size / TILE_SIZE;
  uint64_t write_dev_offset;
  uint64_t write_max_len;
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    // We're writing to the last tile.
    write_dev_offset = inode_dev_offset + INO_OFFSETOF_LAST_TILE_INLINE_DATA(args->key.len, full_tile_count);
    write_max_len = size % TILE_SIZE;
  } else {
    uint32_t tile_addr = read_u24(inode_cur + INO_OFFSETOF_TILE_NO(args->key.len, tile_no));
    write_dev_offset = tile_addr * TILE_SIZE;
    write_max_len = TILE_SIZE;
  }

  uint64_t dest_dev_offset = write_dev_offset + args->written;
  uint64_t dest_max_len = write_max_len - args->written;
  int readno = maybe_read(client_fd, ctx->dev->mmap + dest_dev_offset, dest_max_len);
  if (-1 == readno) {
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  if (readno > 0) {
    device_sync(ctx->dev, dest_dev_offset, dest_dev_offset + readno);
    args->written += readno;
    ASSERT_STATE(args->written <= write_max_len, "wrote past maximum length");
    if (args->written == write_max_len) {
      args->response_written = 0;
      args->response[0] = METHOD_ERROR_OK;
      res = SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
      goto final;
    }
  }
  res = SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;
  goto final;

  final:
  atomic_fetch_sub_explicit(&found->refcount, 1, memory_order_relaxed);
  return res;
}
