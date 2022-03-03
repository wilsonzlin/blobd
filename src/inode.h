#pragma once

/**

INODE
=====

Structure
---------

u24 inode_size
u24 next_inode_tile_or_zero_if_end
u24 next_inode_byte_offset
u64 obj_no
u8 state
u40 size
u8 last_tile_mode
u8 key_len
u8[] key
u24[] tiles
u8[] last_tile_data_if_mode_is_inline

**/

#define INO_OFFSETOF_INODE_SIZE 0
#define INO_OFFSETOF_NEXT_INODE_TILE (INO_OFFSETOF_INODE_SIZE + 3)
#define INO_OFFSETOF_NEXT_INODE_OFFSET (INO_OFFSETOF_NEXT_INODE_TILE + 3)
#define INO_OFFSETOF_OBJ_NO (INO_OFFSETOF_NEXT_INODE_OFFSET + 3)
#define INO_OFFSETOF_STATE (INO_OFFSETOF_OBJ_NO + 8)
#define INO_OFFSETOF_SIZE (INO_OFFSETOF_STATE + 1)
#define INO_OFFSETOF_LAST_TILE_MODE (INO_OFFSETOF_SIZE + 5)
#define INO_OFFSETOF_KEY_LEN (INO_OFFSETOF_LAST_TILE_MODE + 1)
#define INO_OFFSETOF_KEY (INO_OFFSETOF_KEY_LEN + 1)
#define INO_OFFSETOF_TILES(key_len) (INO_OFFSETOF_KEY + (key_len))
#define INO_OFFSETOF_TILE_NO(key_len, tile) (INO_OFFSETOF_KEY + (key_len) + 3 * (tile))
#define INO_OFFSETOF_LAST_TILE_INLINE_DATA(key_len, full_tile_count) (INO_OFFSETOF_TILE_NO(key_len, full_tile_count))

typedef enum {
  INO_STATE_INCOMPLETE = 1,
  INO_STATE_COMMITTED = 2,
  INO_STATE_READY = 3,
  INO_STATE_DELETED = 4,
} ino_state_t;

typedef enum {
  INO_LAST_TILE_MODE_INLINE = 0,
  INO_LAST_TILE_MODE_TILE = 1,
} ino_last_tile_mode_t;
