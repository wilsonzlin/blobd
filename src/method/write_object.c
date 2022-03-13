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
#include "../flush.h"
#include "../inode.h"
#include "../log.h"
#include "../object.h"
#include "../server_client.h"
#include "../server_method_args.h"
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "write_object.h"

// TODO Tune, verify, explain.
#define IMMEDIATE_SYNC_THRESHOLD (4 * 1024 * 1024)

LOGGER("method_write_object");

// Method signature: (u8 key_len, char[] key, u64 obj_no, u64 start).
// Only INCOMPLETE objects can be written to. Objects can only be written in TILE_SIZE chunks, except for the last. Each chunk must start at a multiple of TILE_SIZE.
void method_write_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_write_object_state_t* args = (method_write_object_state_t*) state_raw;

  args->written = 0;
  INIT_STATE_RESPONSE(args, METHOD_WRITE_OBJECT_RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
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
}

svr_client_result_t method_write_object(
  method_ctx_t* ctx,
  void* args_raw,
  svr_client_t* client
) {
  method_write_object_state_t* args = (method_write_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, METHOD_WRITE_OBJECT_RESPONSE_LEN, client, true);

  svr_client_result_t res;
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&ctx->bkts->bucket_locks[args->key.bucket]), "lock bucket");

  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &args->key,
    INO_STATE_INCOMPLETE,
    args->obj_no,
    NULL
  );

  if (!inode_dev_offset) {
    // We close the connection in case the client has already written data, and ending without disconnecting would cause the queued data to be interpreted as the start of the next request.
    // TODO Should we use some framing protocol or format so we don't have to close the connection?
    ts_log(DEBUG, "Connection attempting to write to non-existent object with key %s will be disconnected", args->key.data.bytes);
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

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
  int readno = maybe_read(client->fd, ctx->dev->mmap + dest_dev_offset, dest_max_len);
  if (-1 == readno) {
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
    goto final;
  }
  if (readno > 0) {
    // This is a trade off:
    // - msync() all writes (not just metadata) only at flush times will mean that the device will be totally idle the remaining time and overutilised during flush.
    // - Doing this here means we don't have to make this a manager request, which is single-threaded.
    // - It is better to sync at flush time if the write length is exceptionally small (e.g. less than 1 KiB).
    args->written += readno;
    ASSERT_STATE(args->written <= write_max_len, "wrote past maximum length");
    if (args->written == write_max_len) {
      // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
      args->response_written = 0;
      args->response[0] = METHOD_ERROR_OK;

      if (args->written < IMMEDIATE_SYNC_THRESHOLD) {
        flush_lock_tasks(ctx->flush_state);
        flush_add_write_task(ctx->flush_state, client, args->written);
        flush_unlock_tasks(ctx->flush_state);
        res = SVR_CLIENT_RESULT_AWAITING_FLUSH;
      } else {
        device_sync(ctx->dev, write_dev_offset, write_dev_offset + args->written);
        res = SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
      }

      goto final;
    }
  }
  res = SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;
  goto final;

  final:
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->bkts->bucket_locks[args->key.bucket]), "unlock bucket");
  return res;
}
