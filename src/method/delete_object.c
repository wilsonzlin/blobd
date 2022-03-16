#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "_common.h"
#include "../bucket.h"
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
#include "delete_object.h"

LOGGER("method_delete_object");

method_error_t method_delete_object_parse(
  method_ctx_t* ctx,
  method_delete_object_state_t* state,
  uint8_t* args_cur
) {
  method_error_t key_parse_error = method_common_key_parse(&args_cur, ctx->bkts, &state->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    return key_parse_error;
  }

  state->obj_no_or_zero = consume_u64(&args_cur);

  DEBUG_TS_LOG("delete_object(key=%s, obj_no=%lu)", state->key.data.bytes, state->obj_no_or_zero);

  return METHOD_ERROR_OK;
}

svr_client_result_t method_delete_object_response(
  method_ctx_t* ctx,
  method_delete_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  svr_client_result_t res = SVR_CLIENT_RESULT_WRITE_RESPONSE;
  BUCKET_LOCK_WRITE(ctx->bkts, state->key.bucket);

  uint64_t prev_inode_dev_offset_or_zero_if_head = 0;
  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &state->key,
    // TODO Can we delete PENDING_COMMIT in same journal/flush?
    (state->obj_no_or_zero ? (INO_STATE_INCOMPLETE | INO_STATE_PENDING_COMMIT) : 0) | INO_STATE_READY,
    state->obj_no_or_zero,
    &prev_inode_dev_offset_or_zero_if_head
  );

  if (!inode_dev_offset) {
    produce_u8(&out_response, METHOD_ERROR_NOT_FOUND);
    goto final;
  }

  // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
  produce_u8(&out_response, METHOD_ERROR_OK);
  
  flush_lock_tasks(ctx->flush_state);
  flush_task_reserve_t flush_task = flush_reserve_task(
    ctx->flush_state,
    JOURNAL_ENTRY_DELETE_LEN,
    client,
    inode_dev_offset
  );
  cursor_t* flush_cur = flush_get_reserved_cursor(flush_task);
  produce_u8(&flush_cur, JOURNAL_ENTRY_TYPE_DELETE);
  produce_u48(&flush_cur, inode_dev_offset);
  // Because we hold a lock, we do not need to increment atomically.
  produce_u64(&flush_cur, ctx->stream->next_seq_no++);
  produce_u48(&flush_cur, prev_inode_dev_offset_or_zero_if_head);
  flush_commit_task(ctx->flush_state, flush_task);
  flush_unlock_tasks(ctx->flush_state);
  res = SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE;
  goto final;

  final:
  BUCKET_UNLOCK(ctx->bkts, state->key.bucket);
  return res;
}
