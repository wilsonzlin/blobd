#define _GNU_SOURCE

#include <errno.h>
#include <immintrin.h>
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
#include "inspect_object.h"
#include "../../ext/xxHash/xxhash.h"

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
method_inspect_object_state_t* method_inspect_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_inspect_object_state_t* args = aligned_alloc(64, sizeof(method_inspect_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts->count_log2, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
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
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd);

  ts_log(DEBUG, "inspect_object(key=%s)", args->key.data.bytes);

  cursor_t* inode_cur = NULL;

  bucket_t* bkt = &ctx->bkts->buckets[args->key.bucket];
  if (pthread_rwlock_rdlock(&bkt->lock)) {
    perror("Failed to acquire read lock on bucket");
    exit(EXIT_INTERNAL);
  }
  uint32_t bkt_microtile = bkt->microtile;
  uint32_t bkt_microtile_offset = bkt->microtile_byte_offset;
  while (bkt_microtile) {
    ts_log(DEBUG, "Checking inode at microtile %zu offset %zu", bkt_microtile, bkt_microtile_offset);
    cursor_t* cur = ctx->dev->mmap + (TILE_SIZE * bkt_microtile) + bkt_microtile_offset;
    uint64_t checksum_recorded = read_u64(cur);
    uint32_t inode_size_checksummed = read_u24(cur + 8);
    uint64_t checksum_actual = XXH3_64bits(cur + 8, inode_size_checksummed);
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
    if (ino_key_len != args->key.len) {
      continue;
    }
    // WARNING: Both __m512i arguments must be filled with the same character.
    __m512i ino_key_lower = _mm512_loadu_epi8(ino_key);
    if (_mm512_cmpneq_epi8_mask(ino_key_lower, args->key.data.vecs[0])) {
      continue;
    }
    __m512i ino_key_upper = _mm512_loadu_epi8(ino_key + 64);
    if (_mm512_cmpneq_epi8_mask(ino_key_upper, args->key.data.vecs[1])) {
      continue;
    }
    inode_cur = cur;
    break;
  }
  if (pthread_rwlock_unlock(&bkt->lock)) {
    perror("Failed to release read lock on bucket");
    exit(EXIT_INTERNAL);
  }

  args->response_written = 0;
  cursor_t* res_cur = args->response;
  if (inode_cur == NULL) {
    produce_u8(&res_cur, METHOD_ERROR_NOT_FOUND);
    produce_u8(&res_cur, 0);
    produce_u64(&res_cur, 0);
  } else {
    produce_u8(&res_cur, METHOD_ERROR_OK);
    produce_u8(&res_cur, inode_cur[8 + 3 + 3 + 3]);
    produce_u64(&res_cur, read_u40(inode_cur + 8 + 3 + 3 + 3 + 1));
  }

  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
}
