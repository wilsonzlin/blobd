#define _GNU_SOURCE

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

// [u8 error, u8 state, u64 size].
#define RESPONSE_LEN (1 + 1 + 8)

struct method_inspect_object_state_s {
  // -1 if not prepared yet.
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
};

// Method signature: (u8 key_len, char[] key).
void* method_inspect_object_state_create(
  void* ctx_raw,
  svr_method_args_parser_t* parser
) {
  worker_method_handler_ctx_t* ctx = (worker_method_handler_ctx_t*) ctx_raw;

  method_inspect_object_state_t* args = aligned_alloc(64, sizeof(method_inspect_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  DEBUG_TS_LOG("inspect_object(key=%s)", args->key.data.bytes);

  return args;
}

void method_inspect_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_inspect_object(
  void* ctx_raw,
  void* args_raw,
  int client_fd
) {
  worker_method_handler_ctx_t* ctx = (worker_method_handler_ctx_t*) ctx_raw;
  method_inspect_object_state_t* args = (method_inspect_object_state_t*) args_raw;

  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, true);

  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);

  inode_t* found = method_common_find_inode_in_bucket_for_non_management(
    bkt,
    &args->key,
    ctx->dev,
    INO_STATE_READY,
    0
  );

  args->response_written = 0;
  cursor_t* res_cur = args->response;
  if (found == NULL) {
    produce_u8(&res_cur, METHOD_ERROR_NOT_FOUND);
    produce_u8(&res_cur, 0);
    produce_u64(&res_cur, 0);
  } else {
    cursor_t* inode_cur = INODE_CUR(ctx->dev, found);
    produce_u8(&res_cur, METHOD_ERROR_OK);
    produce_u8(&res_cur, inode_cur[INO_OFFSETOF_STATE]);
    produce_u64(&res_cur, read_u40(inode_cur + INO_OFFSETOF_SIZE));
    atomic_fetch_sub_explicit(&found->refcount, 1, memory_order_relaxed);
  }

  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
}
