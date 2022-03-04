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
#include "delete_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_delete_object");

#define RESPONSE_LEN 1

struct method_delete_object_state_s {
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  uint64_t obj_no_or_zero;
};

// Method signature: (u8 key_len, char[] key, u64 obj_no_or_zero).
method_delete_object_state_t* method_delete_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_delete_object_state_t* args = aligned_alloc(64, sizeof(method_delete_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, RESPONSE_LEN);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->obj_no_or_zero = read_u64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("delete_object(key=%s, obj_no=%lu)", args->key.data.bytes, args->obj_no_or_zero);

  return args;
}

void method_delete_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_delete_object(
  svr_method_handler_ctx_t* ctx,
  method_delete_object_state_t* args,
  int client_fd
) {
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, true);

  // We must acquire a bucket write lock in case someone else tries to commit the object or write to it, or the flusher is currently modifying the list of inodes by processing deletes/commits.
  bool acquired_bkt_lock = false;
  svr_client_result_t res;
  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&bkt->lock), "acquire write lock on bucket");
  acquired_bkt_lock = true;

  ino_state_t allowed_states = (args->obj_no_or_zero ? (INO_STATE_INCOMPLETE | INO_STATE_COMMITTED) : 0) | INO_STATE_READY;
  cursor_t* inode_cur = method_common_find_inode_in_bucket(bkt, &args->key, ctx->dev, allowed_states, args->obj_no_or_zero);

  if (inode_cur == NULL) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }

  inode_cur[INO_OFFSETOF_STATE] = INO_STATE_DELETED;

  buckets_mark_bucket_as_pending_delete_or_commit(ctx->bkts, args->key.bucket);

  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;
  res = SVR_CLIENT_RESULT_AWAITING_FLUSH;

  final:
  if (acquired_bkt_lock && pthread_rwlock_unlock(&bkt->lock)) {
    perror("Failed to release read lock on bucket");
    exit(EXIT_INTERNAL);
  }

  return res;
}
