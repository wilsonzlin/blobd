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
#include "delete_object.h"

LOGGER("method_delete_object");

struct method_delete_object_state_s ;

// Method signature: (u8 key_len, char[] key, u64 obj_no_or_zero).
void method_delete_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_delete_object_state_t* args = (method_delete_object_state_t*) state_raw;

  INIT_STATE_RESPONSE(args, METHOD_DELETE_OBJECT_RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->obj_no_or_zero = read_u64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("delete_object(key=%s, obj_no=%lu)", args->key.data.bytes, args->obj_no_or_zero);
}

svr_client_result_t method_delete_object(
  method_ctx_t* ctx,
  void* args_raw,
  svr_client_t* client
) {
  method_delete_object_state_t* args = (method_delete_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, METHOD_DELETE_OBJECT_RESPONSE_LEN, client, true);

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&ctx->bkts->bucket_locks[args->key.bucket]), "lock bucket");

  uint64_t prev_inode_dev_offset_or_zero_if_head = 0;
  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &args->key,
    // TODO Can we delete PENDING_COMMIT in same journal/flush?
    (args->obj_no_or_zero ? (INO_STATE_INCOMPLETE | INO_STATE_PENDING_COMMIT) : 0) | INO_STATE_READY,
    args->obj_no_or_zero,
    &prev_inode_dev_offset_or_zero_if_head
  );

  if (!inode_dev_offset) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }

  // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;
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

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->bkts->bucket_locks[args->key.bucket]), "unlock bucket");

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
