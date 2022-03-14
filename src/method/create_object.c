#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "_common.h"
#include "../cursor.h"
#include "../device.h"
#include "../exit.h"
#include "../flush.h"
#include "../inode.h"
#include "../log.h"
#include "../object.h"
#include "../server_client.h"
#include "../stream.h"
#include "../tile.h"
#include "../util.h"
#include "../worker.h"
#include "create_object.h"

LOGGER("method_create_object");

method_error_t method_create_object_parse(
  method_ctx_t* ctx,
  method_create_object_state_t* state,
  uint8_t* args_cur
) {
  method_error_t key_parse_error = method_common_key_parse(&args_cur, ctx->bkts, &state->key);
  if (key_parse_error != METHOD_ERROR_OK) {
    return key_parse_error;
  }

  state->size = consume_u64(&args_cur);

  DEBUG_TS_LOG("create_object(key=%s, size=%zu)", state->key.data.bytes, state->size);

  return METHOD_ERROR_OK;
}

typedef enum {
  ALLOC_FULL_TILE,
  ALLOC_MICROTILE_WITH_FULL_LAST_TILE,
  ALLOC_MICROTILE_WITH_INLINE_LAST_TILE,
} alloc_strategy_t;

svr_client_result_t method_create_object_response(
  method_ctx_t* ctx,
  method_create_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
) {
  uint64_t full_tiles = state->size / TILE_SIZE;
  uint64_t last_tile_size = state->size % TILE_SIZE;

  ino_last_tile_mode_t ltm;
  alloc_strategy_t alloc_strategy;
  uint32_t ino_size_excluding_last_tile = INO_OFFSETOF_LAST_TILE_INLINE_DATA(state->key.len, full_tiles);
  uint32_t ino_size_if_inline = ino_size_excluding_last_tile + last_tile_size;
  uint32_t ino_size_excl_any_inline_data;
  uint32_t ino_size;
  if (ino_size_if_inline >= TILE_SIZE) {
    alloc_strategy = ALLOC_MICROTILE_WITH_FULL_LAST_TILE;
    ino_size_excl_any_inline_data = ino_size_excluding_last_tile + 11;
    ino_size = ino_size_excl_any_inline_data;
    ltm = INO_LAST_TILE_MODE_TILE;
  } else if (ino_size_if_inline + INO_OFFSETOF_LAST_TILE_INLINE_DATA(128, 1) >= TILE_SIZE) {
    // Our inode is close to or exactly one tile sized, so directly reserve a tile instead of part of a microtile.
    alloc_strategy = ALLOC_FULL_TILE;
    ino_size_excl_any_inline_data = ino_size_excluding_last_tile;
    ino_size = ino_size_if_inline;
    ltm = INO_LAST_TILE_MODE_INLINE;
  } else {
    alloc_strategy = ALLOC_MICROTILE_WITH_INLINE_LAST_TILE;
    ino_size_excl_any_inline_data = ino_size_excluding_last_tile;
    ino_size = ino_size_if_inline;
    ltm = INO_LAST_TILE_MODE_INLINE;
  }

  freelist_lock(ctx->fl);
  // To prevent microtile metadata corruption, we must append to journal in the same order as microtile allocations.
  // Therefore, we reserve inside freelist lock.
  flush_lock_tasks(ctx->flush_state);
  flush_task_reserve_t flush_task = flush_reserve_task(ctx->flush_state, JOURNAL_ENTRY_CREATE_LEN(ino_size_excl_any_inline_data - INO_OFFSETOF_LAST_TILE_MODE), client, false);
  flush_unlock_tasks(ctx->flush_state);
  uint64_t ino_dev_offset;
  if (alloc_strategy == ALLOC_FULL_TILE) {
    ino_dev_offset = ((uint64_t) freelist_consume_one_tile(ctx->fl)) * TILE_SIZE;
  } else {
    ino_dev_offset = freelist_consume_microtile_space(ctx->fl, ino_size);
  }
  cursor_t* flush_cur = flush_get_reserved_cursor(flush_task);
  cursor_t* inode_cur = flush_cur + JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DATA - INO_OFFSETOF_LAST_TILE_MODE;
  if (full_tiles) {
    freelist_consume_tiles(ctx->fl, full_tiles + ((ltm == INO_LAST_TILE_MODE_TILE) ? 1 : 0), inode_cur + INO_OFFSETOF_TILES(state->key.len));
  }
  freelist_unlock(ctx->fl);

  flush_cur[JOURNAL_ENTRY_OFFSETOF_TYPE] = JOURNAL_ENTRY_TYPE_CREATE;
  write_u48(flush_cur + JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DEV_OFFSET, ino_dev_offset);
  write_u24(flush_cur + JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_LEN, ino_size_excl_any_inline_data - INO_OFFSETOF_LAST_TILE_MODE);

  uint64_t obj_no = atomic_fetch_add_explicit(&ctx->stream->next_obj_no, 1, memory_order_relaxed);

  inode_cur[INO_OFFSETOF_STATE] = INO_STATE_INCOMPLETE;
  inode_cur[INO_OFFSETOF_LAST_TILE_MODE] = ltm;
  write_u40(inode_cur + INO_OFFSETOF_SIZE, state->size);
  write_u64(inode_cur + INO_OFFSETOF_OBJ_NO, obj_no);
  inode_cur[INO_OFFSETOF_KEY_LEN] = state->key.len;
  memcpy(inode_cur + INO_OFFSETOF_KEY, state->key.data.bytes, state->key.len);
  inode_cur[INO_OFFSETOF_KEY_NULL_TERM(state->key.len)] = 0;

  // Set BEFORE possibly committing to flush tasks as it's technically allowed to immediately resume request processing.
  produce_u8(&out_response, METHOD_ERROR_OK);
  produce_u64(&out_response, obj_no);

  flush_lock_tasks(ctx->flush_state);
  flush_commit_task(ctx->flush_state, flush_task);
  flush_unlock_tasks(ctx->flush_state);

  DEBUG_TS_LOG("Using %zu tiles and last tile mode %d", full_tiles, ltm);

  return SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE;
}
