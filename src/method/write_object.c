#define _GNU_SOURCE

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
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
#include "write_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_write_object");

#define RESPONSE_LEN 1

struct method_write_object_state_s {
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  int64_t arg_start;
  int64_t arg_end;
};

// Method signature: (u8 key_len, char[] key, i64 start, i64 end_exclusive).
method_write_object_state_t* method_write_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_write_object_state_t* args = malloc(sizeof(method_write_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts->count_log2, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, RESPONSE_LEN);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->arg_start = read_i64(p);

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->arg_end = read_i64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  return args;
}

void method_write_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_write_object(
  svr_method_handler_ctx_t* ctx,
  method_write_object_state_t* args,
  int client_fd
) {
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd);

  ts_log(DEBUG, "write_object(key=%s, start=%ld, end=%ld)", args->key, args->arg_start, args->arg_end);

  // TODO

  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
