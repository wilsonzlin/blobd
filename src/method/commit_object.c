#include <errno.h>
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
#include "commit_object.h"

LOGGER("method_commit_object");

method_error_t method_commit_object_parse(
  method_ctx_t* ctx,
  method_commit_object_state_t* state,
  uint8_t* args_cur
) {
  (void) ctx;

  state->inode_dev_offset = consume_u64(&args_cur);
  state->obj_no = consume_u64(&args_cur);

  DEBUG_TS_LOG("commit_object(inode_dev_offset=%lu, obj_no=%lu)", state->inode_dev_offset, state->obj_no);

  return METHOD_ERROR_OK;
}

svr_client_result_t method_commit_object_response(
  method_ctx_t* ctx,
  method_commit_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  cursor_t* inode_cur = ctx->dev->mmap + state->inode_dev_offset;

  uint8_t key_len = inode_cur[INO_OFFSETOF_KEY_LEN];
  uint64_t bkt_id = BUCKET_ID_FOR_KEY(
    inode_cur + INO_OFFSETOF_KEY,
    key_len,
    ctx->bkts->key_mask
  );

  BUCKET_LOCK_WRITE(ctx->bkts, bkt_id);

  // Check AFTER acquiring lock in case two requests try to commit the same inode.
  if (
    inode_cur[INO_OFFSETOF_STATE] != INO_STATE_INCOMPLETE ||
    read_u64(inode_cur + INO_OFFSETOF_OBJ_NO) != state->obj_no
  ) {
    produce_u8(&out_response, METHOD_ERROR_NOT_FOUND);
    return SVR_CLIENT_RESULT_WRITE_RESPONSE;
  }

  inode_cur[INO_OFFSETOF_STATE] = INO_STATE_PENDING_COMMIT;

  // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
  produce_u8(&out_response, METHOD_ERROR_OK);

  flush_lock_tasks(ctx->flush_state);
  // If there is a delete, it needs to be in the same journal/flush, so do not call twice.
  flush_task_reserve_t flush_task = flush_reserve_task(
    ctx->flush_state,
    1,
    JOURNAL_ENTRY_COMMIT_LEN,
    client,
    0
  );
  cursor_t* flush_cur = flush_get_reserved_cursor(flush_task);
  produce_u8(&flush_cur, JOURNAL_ENTRY_TYPE_COMMIT);
  produce_u48(&flush_cur, state->inode_dev_offset);
  // Because we hold a lock, we do not need to increment atomically.
  produce_u64(&flush_cur, ctx->stream->next_seq_no++);
  flush_commit_task(ctx->flush_state, flush_task);
  flush_unlock_tasks(ctx->flush_state);

  BUCKET_UNLOCK(ctx->bkts, bkt_id);

  return SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE;
}
