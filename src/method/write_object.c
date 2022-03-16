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
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "write_object.h"

// TODO Tune, verify, explain.
#define IMMEDIATE_SYNC_THRESHOLD (4 * 1024 * 1024)

LOGGER("method_write_object");

method_error_t method_write_object_parse(
  method_ctx_t* ctx,
  method_write_object_state_t* state,
  uint8_t* args_raw
) {
  (void) ctx;

  state->written = 0;

  state->inode_dev_offset = consume_u64(&args_raw);
  state->obj_no = consume_u64(&args_raw);
  state->start = consume_u64(&args_raw);
  if ((state->start % TILE_SIZE) != 0) {
    return METHOD_ERROR_INVALID_START;
  }
  state->len = consume_u64(&args_raw);
  if (state->len > TILE_SIZE) {
    return METHOD_ERROR_INVALID_LENGTH;
  }

  DEBUG_TS_LOG("write_object(inode_dev_offset=%lu, obj_no=%lu, start=%ld)", state->inode_dev_offset, state->obj_no, state->start);

  return METHOD_ERROR_OK;
}

svr_client_result_t method_write_object_response(
  method_ctx_t* ctx,
  method_write_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  (void) ctx;

  cursor_t* inode_cur = ctx->dev->mmap + state->inode_dev_offset;

  if (inode_cur[INO_OFFSETOF_STATE] != INO_STATE_INCOMPLETE) {
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  if (read_u64(inode_cur + INO_OFFSETOF_OBJ_NO) != state->obj_no) {
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  uint64_t size = read_u40(inode_cur + INO_OFFSETOF_SIZE);
  if (state->start >= size) {
    ts_log(DEBUG, "start %lu is past size %lu", state->start, size);
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  uint8_t key_len = inode_cur[INO_OFFSETOF_KEY_LEN];
  uint8_t ltm = inode_cur[INO_OFFSETOF_LAST_TILE_MODE];

  uint64_t tile_no = state->start / TILE_SIZE;
  uint64_t full_tile_count = size / TILE_SIZE;
  uint64_t resolved_write_dev_offset = ltm == INO_LAST_TILE_MODE_INLINE && tile_no == full_tile_count
    // We're writing to the last tile inline.
    ? state->inode_dev_offset + INO_OFFSETOF_LAST_TILE_INLINE_DATA(key_len, full_tile_count)
    : read_u24(inode_cur + INO_OFFSETOF_TILE_NO(key_len, tile_no)) * TILE_SIZE;
  uint64_t resolved_write_len = tile_no == full_tile_count ? size % TILE_SIZE : TILE_SIZE;

  if (state->len != resolved_write_len) {
    ts_log(DEBUG, "write length is unexpected (wanted %lu but requested %lu)", resolved_write_len, state->len);
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  int readno = maybe_read(
    client->fd,
    ctx->dev->mmap + resolved_write_dev_offset + state->written,
    resolved_write_len - state->written
  );
  if (-1 == readno) {
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }
  if (readno > 0) {
    // This is a trade off:
    // - msync() all writes (not just metadata) only at flush times will mean that the device will be totally idle the remaining time and overutilised during flush.
    // - Doing this here means we don't have to make this a manager request, which is single-threaded.
    // - It is better to sync at flush time if the write length is exceptionally small (e.g. less than 1 KiB).
    state->written += readno;
    ASSERT_STATE(state->written <= resolved_write_len, "wrote past maximum length");
    if (state->written == resolved_write_len) {
      // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
      produce_u8(&out_response, METHOD_ERROR_OK);

      if (state->written < IMMEDIATE_SYNC_THRESHOLD) {
        flush_lock_tasks(ctx->flush_state);
        flush_add_write_task(ctx->flush_state, client, resolved_write_len);
        flush_unlock_tasks(ctx->flush_state);
        return SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE;
      } else {
        device_sync(ctx->dev, resolved_write_dev_offset, resolved_write_dev_offset + resolved_write_len);
        return SVR_CLIENT_RESULT_WRITE_RESPONSE;
      }
    }
  }
  return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;
}
