#define _GNU_SOURCE

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
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
#include "../stream.h"
#include "../tile.h"
#include "../util.h"
#include "_common.h"
#include "create_object.h"
#include "../../ext/xxHash/xxhash.h"

LOGGER("method_create_object");

#define RESPONSE_LEN 9

struct method_create_object_state_s {
  int response_written;
  // [u8 error, u64 obj_no].
  uint8_t response[RESPONSE_LEN];
  method_common_key_t key;
  uint64_t size;
};

// Method signature: (u8 key_len, char[] key, u64 size).
method_create_object_state_t* method_create_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
) {
  method_create_object_state_t* args = aligned_alloc(64, sizeof(method_create_object_state_t));
  INIT_STATE_RESPONSE(args, RESPONSE_LEN);
  uint8_t* p = NULL;

  method_error_t key_parse_error = method_common_key_parse(parser, ctx->bkts, &args->key);
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

  DEBUG_TS_LOG("create_object(key=%s, size=%zu)", args->key.data.bytes, args->size);

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
  MAYBE_HANDLE_RESPONSE(args, RESPONSE_LEN, client_fd, true);

  uint64_t full_tiles = args->size / TILE_SIZE;
  uint64_t last_tile_size = args->size % TILE_SIZE;

  ino_last_tile_mode_t ltm;
  alloc_strategy_t alloc_strategy;
  uint32_t ino_size_excluding_last_tile = INO_OFFSETOF_LAST_TILE_INLINE_DATA(args->key.len, full_tiles);
  uint32_t ino_size_if_inline = ino_size_excluding_last_tile + last_tile_size;
  uint32_t ino_size;
  if (ino_size_if_inline >= TILE_SIZE) {
    alloc_strategy = ALLOC_MICROTILE_WITH_FULL_LAST_TILE;
    ino_size = ino_size_excluding_last_tile + 11;
    ltm = INO_LAST_TILE_MODE_TILE;
  } else if (ino_size_if_inline + INO_OFFSETOF_LAST_TILE_INLINE_DATA(128, 1) >= TILE_SIZE) {
    // Our inode is close to or exactly one tile sized, so directly reserve a tile instead of part of a microtile.
    alloc_strategy = ALLOC_FULL_TILE;
    ino_size = ino_size_if_inline;
    ltm = INO_LAST_TILE_MODE_INLINE;
  } else {
    alloc_strategy = ALLOC_MICROTILE_WITH_INLINE_LAST_TILE;
    ino_size = ino_size_if_inline;
    ltm = INO_LAST_TILE_MODE_INLINE;
  }

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&ctx->flush->rwlock), "acquire read lock on flushing");
  // Use 64-bit values as we'll multiply these to get device offset.
  uint64_t ino_addr_tile;
  uint64_t ino_addr_tile_offset;
  if (alloc_strategy == ALLOC_FULL_TILE) {
    ino_addr_tile = freelist_consume_one_tile(ctx->fl);
    ino_addr_tile_offset = 0;
  } else {
    freelist_consumed_microtile_t c = freelist_consume_microtiles(ctx->fl, ino_size);
    ino_addr_tile = c.microtile;
    ino_addr_tile_offset = c.microtile_offset;
  }

  // We can use relaxed memory ordering as we have a lock on flushing.
  uint64_t obj_no = atomic_fetch_add_explicit(&ctx->stream->next_obj_no, 1, memory_order_relaxed);
  DEBUG_TS_LOG("New object number is %lu, will go into bucket %lu", obj_no, args->key.bucket);

  cursor_t* inode_cur = ctx->dev->mmap + (ino_addr_tile * TILE_SIZE) + ino_addr_tile_offset;
  write_u64(inode_cur + INO_OFFSETOF_OBJ_NO, obj_no);
  inode_cur[INO_OFFSETOF_STATE] = INO_STATE_INCOMPLETE;
  write_u40(inode_cur + INO_OFFSETOF_SIZE, args->size);
  inode_cur[INO_OFFSETOF_LAST_TILE_MODE] = ltm;
  inode_cur[INO_OFFSETOF_KEY_LEN] = args->key.len;
  memcpy(inode_cur + INO_OFFSETOF_KEY, args->key.data.bytes, args->key.len);
  inode_cur[INO_OFFSETOF_KEY_NULL_TERM(args->key.len)] = 0;
  if (full_tiles) {
    freelist_consume_tiles(ctx->fl, full_tiles + ((ltm == INO_LAST_TILE_MODE_TILE) ? 1 : 0), inode_cur + INO_OFFSETOF_TILES(args->key.len));
  }
  if (ltm == INO_LAST_TILE_MODE_INLINE) {
    memset(inode_cur + INO_OFFSETOF_LAST_TILE_INLINE_DATA(args->key.len, full_tiles), 0, last_tile_size);
  }
  DEBUG_TS_LOG("Using %zu tiles and last tile mode %d", full_tiles, ltm);

  buckets_mark_bucket_as_dirty(ctx->bkts, args->key.bucket);

  bucket_t* bkt = buckets_get_bucket(ctx->bkts, args->key.bucket);
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&bkt->lock), "acquire write lock on bucket");
  DEBUG_TS_LOG("Next inode is at tile %u offset %u", bkt->tile, bkt->tile_offset);
  write_u24(inode_cur + INO_OFFSETOF_NEXT_INODE_TILE, bkt->tile);
  write_u24(inode_cur + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET, bkt->tile_offset);
  DEBUG_TS_LOG("Wrote inode at tile %u offset %u", ino_addr_tile, ino_addr_tile_offset);
  bkt->tile = ino_addr_tile;
  bkt->tile_offset = ino_addr_tile_offset;
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&bkt->lock), "release write lock on bucket");

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&ctx->flush->rwlock), "release read lock on flushing");

  args->response[0] = METHOD_ERROR_OK;
  write_u64(args->response + 1, obj_no);
  args->response_written = 0;

  return SVR_CLIENT_RESULT_AWAITING_FLUSH;
}
