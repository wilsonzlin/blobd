#define _GNU_SOURCE

#include <errno.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "../cursor.h"
#include "../device.h"
#include "../exit.h"
#include "../inode.h"
#include "../object.h"
#include "../server.h"
#include "../tile.h"
#include "../util.h"
#include "create_object.h"
#include "../../ext/xxHash/xxhash.h"

typedef enum {
  METHOD_CREATE_OBJECT_ERROR__UNKNOWN,
  METHOD_CREATE_OBJECT_ERROR_OK,
  METHOD_CREATE_OBJECT_ERROR_NOT_ENOUGH_ARGS,
  METHOD_CREATE_OBJECT_ERROR_KEY_TOO_LONG,
  METHOD_CREATE_OBJECT_ERROR_TOO_MANY_ARGS,
} method_create_object_error_t;

struct method_create_object_state_s {
  // If this is not METHOD_CREATE_OBJECT_ERROR__UNKNOWN, all other fields are undefined.
  method_create_object_error_t response;
  vec_512i_u8_t key_half_lower;
  vec_512i_u8_t key_half_upper;
  uint64_t key_bucket;
  uint8_t key_len;
  uint64_t size;
};

#define PARSE_OR_ERROR(out_parsed, parser, n, error) \
  out_parsed = svr_method_args_parser_parse(parser, 1); \
  if (p == NULL) { \
    args->response = error; \
    return args; \
  }

// Method signature: (u8 key_len, char[] key, u64 size).
method_create_object_state_t* method_create_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_create_object_state_t* args = malloc(sizeof(method_create_object_state_t));
  args->response = METHOD_CREATE_OBJECT_ERROR__UNKNOWN;
  uint8_t* p = NULL;

  PARSE_OR_ERROR(p, parser, 1, METHOD_CREATE_OBJECT_ERROR_NOT_ENOUGH_ARGS);
  args->key_len = *p;
  if (args->key_len > 128) {
    args->response = METHOD_CREATE_OBJECT_ERROR_KEY_TOO_LONG;
    return args;
  }
  uint8_t key[128];
  PARSE_OR_ERROR(p, parser, args->key_len, METHOD_CREATE_OBJECT_ERROR_NOT_ENOUGH_ARGS);
  memcpy(key, p, args->key_len);
  memcpy(&args->key_half_lower.elems[0], key, 64);
  memcpy(&args->key_half_upper.elems[0], &key[64], 64);
  args->key_bucket = XXH3_64bits(key, args->key_len) & ((1 << ctx->bkts->count_log2) - 1);
  PARSE_OR_ERROR(p, parser, 8, METHOD_CREATE_OBJECT_ERROR_NOT_ENOUGH_ARGS);
  args->size = read_u64(p);
  if (!svr_method_args_parser_end(parser)) {
    args->response = METHOD_CREATE_OBJECT_ERROR_TOO_MANY_ARGS;
    return args;
  }
  return args;
}

void method_create_object_state_destroy(void* state) {
  free(state);
}

svr_client_result_t method_create_object(
  svr_method_handler_ctx_t* ctx,
  method_create_object_state_t* args,
  int client_fd
) {
  if (args->response != METHOD_CREATE_OBJECT_ERROR__UNKNOWN) {
    uint8_t buf[1] = {args->response};
    int writeno = write(client_fd, buf, 1);
    if (1 == writeno) {
      return SVR_CLIENT_RESULT_END;
    }
    if (-1 == writeno && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
    }
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
  }

  size_t full_tiles = args->size / TILE_SIZE;
  size_t last_tile_size = args->size % TILE_SIZE;
  ino_last_tile_mode_t last_tile_mode;
  size_t last_tile_metadata_size;
  if (last_tile_size <= INO_LAST_TILE_THRESHOLD) {
    last_tile_mode = INO_LAST_TILE_MODE_INLINE;
    last_tile_metadata_size = last_tile_size;
  } else if (last_tile_size >= (TILE_SIZE - INO_LAST_TILE_THRESHOLD)) {
    last_tile_mode = INO_LAST_TILE_MODE_TILE;
    last_tile_metadata_size = 3;
  } else {
    last_tile_mode = INO_LAST_TILE_MODE_MICROTILE;
    last_tile_metadata_size = 6;
  }
  size_t inode_size = 8 + 3 + 3 + 1 + 5 + 1 + args->key_len + (full_tiles * 11) + 1 + last_tile_metadata_size;

  if (pthread_rwlock_rdlock(&ctx->flush->rwlock)) {
    perror("Failed to acquire read lock on flushing");
    exit(EXIT_INTERNAL);
  }
  freelist_consumed_microtile_t inode_addr = freelist_consume_microtiles(ctx->fl, inode_size);
  cursor_t* inode_cur = ctx->dev->mmap + (inode_addr.microtile * TILE_SIZE) + inode_addr.microtile_offset;
  cursor_t* cur = inode_cur + 8 + 3 + 3;
  produce_u8(&cur, INO_STATE_OK);
  produce_u40(&cur, args->size);
  produce_u8(&cur, args->key_len);
  produce_n(&cur, &args->key_half_lower.elems[0], min(args->key_len, 64));
  if (args->key_len > 64) {
    produce_n(&cur, &args->key_half_upper.elems[0], args->key_len - 64);
  }
  produce_u8(&cur, last_tile_mode);
  if (full_tiles) {
    freelist_consume_tiles(ctx->fl, full_tiles + (last_tile_mode == INO_LAST_TILE_MODE_TILE), &cur);
  }
  if (last_tile_mode == INO_LAST_TILE_MODE_TILE) {
    cur -= 8;
  } else if (last_tile_mode == INO_LAST_TILE_MODE_MICROTILE) {
    freelist_consumed_microtile_t l = freelist_consume_microtiles(ctx->fl, last_tile_size);
    produce_u24(&cur, l.microtile);
    produce_u24(&cur, l.microtile_offset);
  }

  uint_least64_t bkt_ptr_new = (inode_addr.microtile << 24) | (inode_addr.microtile_offset);
  uint_least64_t bkt_ptr_existing = ctx->bkts->bucket_pointers[args->key_bucket];
  while (!atomic_compare_exchange_weak(&ctx->bkts->bucket_pointers[args->key_bucket], &bkt_ptr_existing, bkt_ptr_new)) {}

  if (pthread_rwlock_wrlock(&ctx->bkts->dirty_sixteen_pointers_rwlock)) {
    perror("Failed to acquire write lock on buckets");
    exit(EXIT_INTERNAL);
  }
  for (
    size_t o = args->key_bucket, i = ctx->bkts->dirty_sixteen_pointers_layer_count;
    i > 0;
    o /= 64, i--
  ) {
    ctx->bkts->dirty_sixteen_pointers[i - 1][o / 64] |= (1 << (o % 64));
  }
  if (pthread_rwlock_unlock(&ctx->bkts->dirty_sixteen_pointers_rwlock)) {
    perror("Failed to release write lock on buckets");
    exit(EXIT_INTERNAL);
  }

  write_u24(inode_cur + 8, bkt_ptr_existing >> 24);
  write_u24(inode_cur + 8 + 3, bkt_ptr_existing & ((1 << 24) - 1));
  uint64_t hash = XXH3_64bits(inode_cur + 8, inode_size - 8);
  write_u64(inode_cur, hash);

  if (pthread_rwlock_unlock(&ctx->flush->rwlock)) {
    perror("Failed to release read lock on flushing");
    exit(EXIT_INTERNAL);
  }

  args->response = METHOD_CREATE_OBJECT_ERROR_OK;

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
