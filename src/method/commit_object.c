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
#include "../server_method_args.h"
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "commit_object.h"

LOGGER("method_commit_object");

// Method signature: (u8 key_len, u64 dev_offset, u64 obj_no).
void method_commit_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_commit_object_state_t* args = (method_commit_object_state_t*) state_raw;

  (void) ctx;

  INIT_STATE_RESPONSE(args, METHOD_COMMIT_OBJECT_RESPONSE_LEN);
  uint8_t* p = NULL;

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->inode_dev_offset = read_u64(p);

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->obj_no = read_u64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("commit_object(inode_dev_offset=%lu, obj_no=%lu)", args->inode_dev_offset, args->obj_no);
}

svr_client_result_t method_commit_object(
  method_ctx_t* ctx,
  void* args_raw,
  svr_client_t* client
) {
  method_commit_object_state_t* args = (method_commit_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, METHOD_COMMIT_OBJECT_RESPONSE_LEN, client, true);

  cursor_t* inode_cur = ctx->dev->mmap + args->inode_dev_offset;
  uint8_t key_len = inode_cur[INO_OFFSETOF_KEY_LEN];
  uint64_t bkt_id = BUCKET_ID_FOR_KEY(
    inode_cur + INO_OFFSETOF_KEY,
    key_len,
    ctx->bkts->key_mask
  );

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&ctx->bkts->bucket_locks[bkt_id]), "lock bucket");

  if (
    inode_cur[INO_OFFSETOF_STATE] != INO_STATE_INCOMPLETE ||
    read_u64(inode_cur + INO_OFFSETOF_OBJ_NO) != args->obj_no
  ) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }
  inode_cur[INO_OFFSETOF_STATE] = INO_STATE_PENDING_COMMIT;

  // Ensure object is found first before deleting other objects.
  // We should only ever find at most one other inode with the same key and with state READY or PENDING_COMMIT.
  cursor_t* bkt_head_cur = ctx->dev->mmap + ctx->bkts->dev_offset + BUCKETS_OFFSETOF_BUCKET(bkt_id);
  uint64_t delete_prev_inode_dev_offset = 0;
  uint64_t delete_inode_dev_offset = read_u48(bkt_head_cur);
  while (delete_inode_dev_offset) {
    cursor_t* cur = ctx->dev->mmap + delete_inode_dev_offset;
    // TODO Can we delete PENDING_COMMIT in same journal/flush?
    if (
      delete_inode_dev_offset != args->inode_dev_offset &&
      (cur[INO_OFFSETOF_STATE] & (INO_STATE_READY | INO_STATE_PENDING_COMMIT)) &&
      (cur[INO_OFFSETOF_KEY_LEN] == key_len) &&
      (0 == memcmp(cur + INO_OFFSETOF_KEY, inode_cur + INO_OFFSETOF_KEY, key_len))
    ) {
      break;
    }
    delete_prev_inode_dev_offset = delete_inode_dev_offset;
    delete_inode_dev_offset = read_u48(cur + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET);
  }

  // Set BEFORE possibly adding to flush tasks as it's technically allowed to immediately resume request processing.
  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;


  flush_lock_tasks(ctx->flush_state);
  // If there is a delete, it needs to be in the same journal/flush, so do not call twice.
  flush_task_reserve_t flush_task = flush_reserve_task(
    ctx->flush_state,
    JOURNAL_ENTRY_COMMIT_LEN + (delete_inode_dev_offset ? JOURNAL_ENTRY_DELETE_LEN : 0),
    client,
    delete_inode_dev_offset
  );
  cursor_t* flush_cur = flush_get_reserved_cursor(flush_task);
  produce_u8(&flush_cur, JOURNAL_ENTRY_TYPE_COMMIT);
  produce_u48(&flush_cur, args->inode_dev_offset);
  // Because we hold a lock, we do not need to increment atomically.
  produce_u64(&flush_cur, ctx->stream->next_seq_no++);
  if (delete_inode_dev_offset) {
    produce_u8(&flush_cur, JOURNAL_ENTRY_TYPE_DELETE);
    produce_u48(&flush_cur, delete_inode_dev_offset);
    // Because we hold a lock, we do not need to increment atomically.
    produce_u64(&flush_cur, ctx->stream->next_seq_no++);
    produce_u48(&flush_cur, delete_prev_inode_dev_offset);
  }
  flush_commit_task(ctx->flush_state, flush_task);
  flush_unlock_tasks(ctx->flush_state);

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->bkts->bucket_locks[bkt_id]), "unlock bucket");

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
