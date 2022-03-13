#include <errno.h>
#include <immintrin.h>
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
#include "inspect_object.h"

LOGGER("method_create_object");

// Method signature: (u8 key_len, char[] key).
void method_inspect_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_inspect_object_state_t* args = (method_inspect_object_state_t*) state_raw;

  INIT_STATE_RESPONSE(args, METHOD_INSPECT_OBJECT_RESPONSE_LEN);

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("inspect_object(key=%s)", args->key.data.bytes);
}

svr_client_result_t method_inspect_object(
  method_ctx_t* ctx,
  void* args_raw,
  svr_client_t* client
) {
  method_inspect_object_state_t* args = (method_inspect_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, METHOD_INSPECT_OBJECT_RESPONSE_LEN, client, true);

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&ctx->bkts->bucket_locks[args->key.bucket]), "lock bucket");

  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &args->key,
    INO_STATE_READY,
    0,
    NULL
  );

  args->response_written = 0;
  cursor_t* res_cur = args->response;
  if (!inode_dev_offset) {
    produce_u8(&res_cur, METHOD_ERROR_NOT_FOUND);
    produce_u8(&res_cur, 0);
    produce_u64(&res_cur, 0);
  } else {
    cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;
    produce_u8(&res_cur, METHOD_ERROR_OK);
    produce_u8(&res_cur, inode_cur[INO_OFFSETOF_STATE]);
    produce_u64(&res_cur, read_u40(inode_cur + INO_OFFSETOF_SIZE));
  }

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->bkts->bucket_locks[args->key.bucket]), "unlock bucket");

  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
}
