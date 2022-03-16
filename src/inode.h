#pragma once

/**

INODE
=====

We use a null terminator after the key so that we can use it directly from mmap'ed memory without having to allocate memory, copy the key, and then add a null terminator. This is useful for printing, khash sets, and other libraries and APIs that except C-style strings.

Structure
---------

The ordering is important, as we want to avoid multiple read() syscalls. Therefore we position `size`, `obj_no`, `key_len`, and `key` together, so it's possible to get all that data in one call, and optionally without `size`.

u48 next_inode_dev_offset_or_zero_if_end
u8 state
u8 last_tile_mode
u40 size
u64 obj_no
u8 key_len
u8[] key
u8 key_null_terminator
u24[] tiles
u8[] last_tile_data_if_mode_is_inline

**/

#include <stdatomic.h>
#include <stdint.h>
#include "tile.h"

#define INO_OFFSETOF_NEXT_INODE_DEV_OFFSET 0
#define INO_OFFSETOF_STATE (INO_OFFSETOF_NEXT_INODE_DEV_OFFSET + 6)
#define INO_OFFSETOF_LAST_TILE_MODE (INO_OFFSETOF_STATE + 1)
#define INO_OFFSETOF_SIZE (INO_OFFSETOF_LAST_TILE_MODE + 1)
#define INO_OFFSETOF_OBJ_NO (INO_OFFSETOF_SIZE + 5)
#define INO_OFFSETOF_KEY_LEN (INO_OFFSETOF_OBJ_NO + 8)
#define INO_OFFSETOF_KEY (INO_OFFSETOF_KEY_LEN + 1)
#define INO_OFFSETOF_KEY_NULL_TERM(key_len) (INO_OFFSETOF_KEY + (key_len))
#define INO_OFFSETOF_TILE_NO(key_len, tile) (INO_OFFSETOF_KEY_NULL_TERM(key_len) + 1 + 3 * (tile))
#define INO_OFFSETOF_TILES(key_len) INO_OFFSETOF_TILE_NO(key_len, 0)
#define INO_OFFSETOF_LAST_TILE_INLINE_DATA(key_len, full_tile_count) INO_OFFSETOF_TILE_NO(key_len, full_tile_count)
#define INO_SIZE(key_len, full_tile_count, inline_data_len) (INO_OFFSETOF_LAST_TILE_INLINE_DATA(key_len, full_tile_count) + (inline_data_len))

typedef enum {
  INO_STATE_DELETED = 1 << 0,
  INO_STATE_INCOMPLETE = 1 << 1,
  INO_STATE_PENDING_COMMIT = 1 << 2,
  INO_STATE_PENDING_DELETE = 1 << 3,
  INO_STATE_READY = 1 << 4,
} ino_state_t;

#define INODE_STATE_IS_VALID(x) ( \
  (x) == INO_STATE_DELETED || \
  (x) == INO_STATE_INCOMPLETE || \
  (x) == INO_STATE_PENDING_COMMIT || \
  (x) == INO_STATE_PENDING_DELETE || \
  (x) == INO_STATE_READY \
)

typedef enum {
  INO_LAST_TILE_MODE_INLINE = 0,
  INO_LAST_TILE_MODE_TILE = 1,
} ino_last_tile_mode_t;

#define INODE_LAST_TILE_MODE_IS_VALID(x) ( \
  (x) == INO_LAST_TILE_MODE_INLINE || \
  (x) == INO_LAST_TILE_MODE_TILE \
)

#define INODE_TILE_COUNT(inode_cur) ((read_u40((inode_cur) + INO_OFFSETOF_SIZE) / TILE_SIZE) + ((inode_cur)[INO_OFFSETOF_STATE] == INO_LAST_TILE_MODE_TILE ? 1 : 0))

#ifdef TURBOSTORE_DEBUG
#define DEBUG_ASSERT_INODE_IS_VALID(dev, inode_dev_offset) { \
  cursor_t* cur = (dev)->mmap + (inode_dev_offset); \
  DEBUG_ASSERT_STATE( \
    INODE_STATE_IS_VALID(cur[INO_OFFSETOF_STATE]), \
    "inode at device offset %lu does not have a valid state (%u)", \
    inode_dev_offset, \
    cur[INO_OFFSETOF_STATE] \
  ); \
  DEBUG_ASSERT_STATE( \
    INODE_LAST_TILE_MODE_IS_VALID(cur[INO_OFFSETOF_LAST_TILE_MODE]), \
    "inode at device offset %lu does not have a valid last tile mode (%u)", \
    inode_dev_offset, \
    cur[INO_OFFSETOF_LAST_TILE_MODE] \
  ); \
  DEBUG_ASSERT_STATE( \
    cur[INO_OFFSETOF_KEY_LEN] > 0 && cur[INO_OFFSETOF_KEY_LEN] <= 128, \
    "inode at device offset %lu does not have valid key length (%u)", \
    inode_dev_offset, \
    cur[INO_OFFSETOF_KEY_LEN] \
  ); \
  DEBUG_ASSERT_STATE( \
    cur[INO_OFFSETOF_KEY_NULL_TERM(cur[INO_OFFSETOF_KEY_LEN])] == 0, \
    "inode at device offset %lu does not have key null terminator", \
    inode_dev_offset \
  ); \
}
#else
#define DEBUG_ASSERT_INODE_IS_VALID(dev, inode_dev_offset) ((void) 0)
#endif
