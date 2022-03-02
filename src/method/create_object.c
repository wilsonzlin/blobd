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
#include "create_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_create_object");

#define RESPONSE_LEN 1

struct method_create_object_state_s {
  int response_written;
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  uint64_t size;
};

// Method signature: (u8 key_len, char[] key, u64 size).
method_create_object_state_t* method_create_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_create_object_state_t* args = malloc(sizeof(method_create_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts->count_log2, &args->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    PARSE_ERROR(args, key_parse_error);
  }

  if ((p = svr_method_args_parser_parse(parser, 8)) == NULL) {
    PARSE_ERROR(args, METHOD_ERROR_NOT_ENOUGH_ARGS);
  }
  args->size = read_u64(p);

  if (!svr_method_args_parser_end(parser)) {
    PARSE_ERROR(args, METHOD_ERROR_TOO_MANY_ARGS);
  }

  return args;
}

void method_create_object_state_destroy(void* state) {
  free(state);
}

typedef enum {
  ALLOC_FULL_TILE,
  ALLOC_MICROTILE_WITH_FULL_LAST_TILE,
  ALLOC_MICROTILE_WITH_INLINE_LAST_TILE,
} alloc_strategy_t;

svr_client_result_t method_create_object(
  svr_method_handler_ctx_t* ctx,
  method_create_object_state_t* args,
  int client_fd
) {
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd);

  ts_log(DEBUG, "create_object(key=%s, size=%zu)", args->key, args->size);
  size_t full_tiles = args->size / TILE_SIZE;
  size_t last_tile_size = args->size % TILE_SIZE;

  ino_last_tile_mode_t ltm;
  alloc_strategy_t alloc_strategy;
  size_t ino_size_excluding_last_tile = 8 + 3 + 3 + 3 + 1 + 5 + 1 + args->key.len + 1 + (full_tiles * 11);
  size_t ino_size_if_inline = ino_size_excluding_last_tile + 8 + last_tile_size;
  size_t ino_size;
  size_t ino_size_checksummed;
  if (ino_size_if_inline >= TILE_SIZE) {
    alloc_strategy = ALLOC_MICROTILE_WITH_FULL_LAST_TILE;
    ino_size = ino_size_excluding_last_tile + 11;
    ino_size_checksummed = ino_size - 8;
    ltm = INO_LAST_TILE_MODE_TILE;
  } else if (ino_size_if_inline + 8 + 3 + 3 + 3 + 1 + 5 + 1 + 128 + 1 + 11 >= TILE_SIZE) {
    alloc_strategy = ALLOC_FULL_TILE;
    ino_size = ino_size_if_inline;
    ino_size_checksummed = ino_size_excluding_last_tile + 8 - 8;
    ltm = INO_LAST_TILE_MODE_INLINE;
  } else {
    alloc_strategy = ALLOC_MICROTILE_WITH_INLINE_LAST_TILE;
    ino_size = ino_size_if_inline;
    ino_size_checksummed = ino_size_excluding_last_tile + 8 - 8;
    ltm = INO_LAST_TILE_MODE_INLINE;
  }

  if (pthread_rwlock_rdlock(&ctx->flush->rwlock)) {
    perror("Failed to acquire read lock on flushing");
    exit(EXIT_INTERNAL);
  }
  // Use 64-bit values as we'll multiply these to get device offset.
  uint64_t ino_addr_tile;
  uint64_t ino_addr_tile_byte_offset;
  if (alloc_strategy == ALLOC_FULL_TILE) {
    ino_addr_tile = freelist_consume_one_tile(ctx->fl);
    ino_addr_tile_byte_offset = 0;
  } else {
    freelist_consumed_microtile_t c = freelist_consume_microtiles(ctx->fl, ino_size);
    ino_addr_tile = c.microtile;
    ino_addr_tile_byte_offset = c.microtile_offset;
  }

  cursor_t* inode_cur = ctx->dev->mmap + (ino_addr_tile * TILE_SIZE) + ino_addr_tile_byte_offset;
  write_u24(inode_cur + 8, ino_size_checksummed);
  cursor_t* cur = inode_cur + 8 + 3 + 3 + 3;
  produce_u8(&cur, INO_STATE_INCOMPLETE);
  produce_u40(&cur, args->size);
  produce_u8(&cur, args->key.len);
  produce_n(&cur, args->key.data.bytes, args->key.len);
  produce_u8(&cur, ltm);
  if (full_tiles) {
    freelist_consume_tiles(ctx->fl, full_tiles + ((ltm == INO_LAST_TILE_MODE_TILE) ? 1 : 0), &cur);
  }
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    memset(cur + 8, 0, last_tile_size);
    uint64_t inline_data_hash = XXH3_64bits(cur + 8, last_tile_size);
    produce_u64(&cur, inline_data_hash);
  }
  ts_log(DEBUG, "Using %zu tiles and last tile mode %d", full_tiles, ltm);

  if (pthread_rwlock_wrlock(&ctx->bkts->dirty_sixteen_pointers_rwlock)) {
    perror("Failed to acquire write lock on buckets");
    exit(EXIT_INTERNAL);
  }
  for (
    size_t o = args->key.bucket / 16, i = ctx->bkts->dirty_sixteen_pointers_layer_count;
    i > 0;
    o /= 64, i--
  ) {
    ctx->bkts->dirty_sixteen_pointers[i - 1][o / 64] |= (1llu << (o % 64));
  }
  if (pthread_rwlock_unlock(&ctx->bkts->dirty_sixteen_pointers_rwlock)) {
    perror("Failed to release write lock on buckets");
    exit(EXIT_INTERNAL);
  }

  bucket_t* bkt = ctx->bkts->buckets + args->key.bucket;
  if (pthread_rwlock_wrlock(&bkt->lock)) {
    perror("Failed to acquire write lock on bucket");
    exit(EXIT_INTERNAL);
  }
  ts_log(DEBUG, "Next inode is at microtile %u and offset %u", bkt->microtile, bkt->microtile_byte_offset);
  write_u24(inode_cur + 8 + 3, bkt->microtile);
  write_u24(inode_cur + 8 + 3 + 3, bkt->microtile_byte_offset);
  uint64_t hash = XXH3_64bits(inode_cur + 8, ino_size_checksummed);
  write_u64(inode_cur, hash);
  ts_log(DEBUG, "Wrote inode");
  bkt->microtile = ino_addr_tile;
  bkt->microtile_byte_offset = ino_addr_tile_byte_offset;
  if (pthread_rwlock_unlock(&bkt->lock)) {
    perror("Failed to release write lock on bucket");
    exit(EXIT_INTERNAL);
  }

  if (pthread_rwlock_unlock(&ctx->flush->rwlock)) {
    perror("Failed to release read lock on flushing");
    exit(EXIT_INTERNAL);
  }

  args->response[0] = METHOD_ERROR_OK;
  args->response_written = 0;

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
