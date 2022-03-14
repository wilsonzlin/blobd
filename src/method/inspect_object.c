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
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "inspect_object.h"

LOGGER("method_create_object");

method_error_t method_inspect_object_parse(
  method_ctx_t* ctx,
  method_inspect_object_state_t* state,
  uint8_t* args_cur
) {
  method_error_t key_parse_error = method_common_key_parse(&args_cur, ctx->bkts, &state->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    return key_parse_error;
  }

  DEBUG_TS_LOG("inspect_object(key=%s)", state->key.data.bytes);

  return METHOD_ERROR_OK;
}

svr_client_result_t method_inspect_object_response(
  method_ctx_t* ctx,
  method_inspect_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  (void) client;

  BUCKET_LOCK_READ(ctx->bkts, state->key.bucket);

  uint64_t inode_dev_offset = method_common_find_inode_in_bucket(
    ctx->dev,
    ctx->bkts,
    &state->key,
    INO_STATE_READY,
    0,
    NULL
  );

  if (!inode_dev_offset) {
    produce_u8(&out_response, METHOD_ERROR_NOT_FOUND);
    produce_u8(&out_response, 0);
    produce_u64(&out_response, 0);
  } else {
    cursor_t* inode_cur = ctx->dev->mmap + inode_dev_offset;
    produce_u8(&out_response, METHOD_ERROR_OK);
    produce_u8(&out_response, inode_cur[INO_OFFSETOF_STATE]);
    produce_u64(&out_response, read_u40(inode_cur + INO_OFFSETOF_SIZE));
  }

  BUCKET_UNLOCK(ctx->bkts, state->key.bucket);
  return SVR_CLIENT_RESULT_WRITE_RESPONSE;
}
