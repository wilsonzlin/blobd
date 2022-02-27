#define _GNU_SOURCE

#include <errno.h>
#include <immintrin.h>
#include <stdatomic.h>
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
#include "inspect_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_create_object");

typedef enum {
  METHOD_INSPECT_OBJECT_ERROR_OK,
  METHOD_INSPECT_OBJECT_ERROR_NOT_ENOUGH_ARGS,
  METHOD_INSPECT_OBJECT_ERROR_KEY_TOO_LONG,
  METHOD_INSPECT_OBJECT_ERROR_TOO_MANY_ARGS,
  METHOD_INSPECT_OBJECT_ERROR_NOT_FOUND,
} method_inspect_object_error_t;

// [u8 error, u8 state, u64 size].
#define RESPONSE_LEN (1 + 1 + 8)

struct method_inspect_object_state_s {
  // -1 if not prepared yet.
  int response_written;
  uint8_t response[RESPONSE_LEN];
  vec_512i_u8_t key_half_lower;
  vec_512i_u8_t key_half_upper;
  uint64_t key_bucket;
  uint8_t key_len;
};

#define PARSE_OR_ERROR(out_parsed, parser, n, error) \
  out_parsed = svr_method_args_parser_parse(parser, n); \
  if (p == NULL) { \
    args->response_written = 0; \
    args->response[0] = error; \
    return args; \
  }

// Method signature: (u8 key_len, char[] key).
method_inspect_object_state_t* method_inspect_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_inspect_object_state_t* args = aligned_alloc(64, sizeof(method_inspect_object_state_t));
  memset(args->response, 0, RESPONSE_LEN);
  args->response_written = -1;
  uint8_t* p = NULL;

  PARSE_OR_ERROR(p, parser, 1, METHOD_INSPECT_OBJECT_ERROR_NOT_ENOUGH_ARGS);
  args->key_len = *p;
  if (args->key_len > 128) {
    args->response_written = 0;
    args->response[0] = METHOD_INSPECT_OBJECT_ERROR_KEY_TOO_LONG;
    return args;
  }
  uint8_t key[128];
  memset(key, 0, 128);
  PARSE_OR_ERROR(p, parser, args->key_len, METHOD_INSPECT_OBJECT_ERROR_NOT_ENOUGH_ARGS);
  memcpy(key, p, args->key_len);
  memcpy(args->key_half_lower.elems, key, 64);
  memcpy(args->key_half_upper.elems, key + 64, 64);
  args->key_bucket = XXH3_64bits(key, args->key_len) & ((1llu << ctx->bkts->count_log2) - 1);
  if (!svr_method_args_parser_end(parser)) {
    args->response_written = 0;
    args->response[0] = METHOD_INSPECT_OBJECT_ERROR_TOO_MANY_ARGS;
    return args;
  }
  return args;
}

void method_inspect_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_inspect_object(
  svr_method_handler_ctx_t* ctx,
  method_inspect_object_state_t* args,
  int client_fd
) {
  if (args->response_written >= 0) {
    int writeno = write(client_fd, args->response + args->response_written, RESPONSE_LEN - args->response_written);
    if (-1 == writeno && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
    }
    if (writeno > 0) {
      if ((args->response_written += writeno) == RESPONSE_LEN) {
        return SVR_CLIENT_RESULT_END;
      } else {
        return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
      }
    }
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  ts_log(DEBUG, "inspect_object(key=%.64s%.64s)", args->key_half_lower.elems, args->key_half_upper.elems);

  cursor_t* inode_cur = NULL;

  uint_least64_t bkt_ptr_raw = atomic_load(ctx->bkts->bucket_pointers + args->key_bucket);
  uint32_t bkt_microtile = bkt_ptr_raw >> 24;
  uint32_t bkt_microtile_offset = bkt_ptr_raw & ((1llu << 24) - 1);
  while (bkt_microtile) {
    ts_log(DEBUG, "Checking inode at microtile %zu offset %zu", bkt_microtile, bkt_microtile_offset);
    cursor_t* cur = ctx->dev->mmap + (TILE_SIZE * bkt_microtile) + bkt_microtile_offset;
    uint64_t checksum_recorded = read_u64(cur);
    uint32_t inode_size_excluding_xxhash = read_u24(cur + 8);
    uint64_t checksum_actual = XXH3_64bits(cur + 8, inode_size_excluding_xxhash);
    if (checksum_recorded != checksum_actual) {
      CORRUPT("inode at microtile %u offset %u has recorded checksum %x but data checksums to %x", bkt_microtile, bkt_microtile_offset, checksum_recorded, checksum_actual);
    }
    bkt_microtile = read_u24(cur + 8 + 3);
    bkt_microtile_offset = read_u24(cur + 8 + 3 + 3);
    // TODO Check state.
    uint8_t ino_key_len = cur[8 + 3 + 3 + 3 + 1 + 5];
    uint8_t ino_key[129];
    memcpy(ino_key, cur + 8 + 3 + 3 + 3 + 1 + 5 + 1, ino_key_len);
    ino_key[ino_key_len] = 0;
    ts_log(DEBUG, "Inode has key %s", ino_key);
    if (ino_key_len != args->key_len) {
      continue;
    }
    // WARNING: Both __m512i arguments must be filled with the same character.
    __m512i ino_key_lower = _mm512_loadu_epi8(ino_key);
    if (_mm512_cmpneq_epi8_mask(ino_key_lower, args->key_half_lower.vec)) {
      continue;
    }
    __m512i ino_key_upper = _mm512_loadu_epi8(ino_key + 64);
    if (_mm512_cmpneq_epi8_mask(ino_key_upper, args->key_half_upper.vec)) {
      continue;
    }
    inode_cur = cur;
    break;
  }

  cursor_t* res_cur = args->response;

  if (inode_cur == NULL) {
    produce_u8(&res_cur, METHOD_INSPECT_OBJECT_ERROR_NOT_FOUND);
    produce_u8(&res_cur, 0);
    produce_u64(&res_cur, 0);
  } else {
    produce_u8(&res_cur, METHOD_INSPECT_OBJECT_ERROR_OK);
    produce_u8(&res_cur, inode_cur[8 + 3 + 3 + 3]);
    produce_u64(&res_cur, read_u40(inode_cur + 8 + 3 + 3 + 3 + 1));
  }

  args->response_written = 0;

  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
}
