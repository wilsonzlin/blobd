#pragma once

/**

INODE
=====

Structure
---------

u64 xxhash_excluding_optional_inline_data
u24 inode_size_excluding_xxhash_and_optional_last_tile_inline_data
u24 next_inode_tile_or_zero_if_end
u24 next_inode_byte_offset
u8 state
u40 size
u8 key_len
u8[] key
u8 last_tile_mode
{
  u24 tile
  u64 xxhash_of_contents
}[] tiles
optional {
  u64 xxhash_of_contents
  u8[] inline_data
} last_tile_if_mode_is_inline

**/

typedef enum {
  INO_STATE_INCOMPLETE = 1,
  INO_STATE_READY = 2,
  INO_STATE_CORRUPT = 3,
  INO_STATE_DELETED = 4,
} ino_state_t;

typedef enum {
  INO_LAST_TILE_MODE_INLINE = 0,
  INO_LAST_TILE_MODE_TILE = 1,
} ino_last_tile_mode_t;
