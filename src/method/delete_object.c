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

  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);

  inode_t* prev = NULL;
  inode_t* found = NULL;
  METHOD_COMMON_ITERATE_INODES_IN_BUCKET_FOR_MANAGEMENT(bkt, &args->key, ctx->dev, (args->obj_no_or_zero ? INO_STATE_INCOMPLETE : 0) | INO_STATE_READY, args->obj_no_or_zero, bkt_ino, prev = bkt_ino) {
    found = bkt_ino;
    break;
  }

  if (found == NULL) {
    ERROR_RESPONSE(METHOD_ERROR_NOT_FOUND);
  }

  flush_mark_inode_for_awaiting_deletion(ctx->flush_state, args->key.bucket, prev, found);

  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;
  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
