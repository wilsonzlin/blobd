#pragma once

/**

INODE
=====

Structure
---------

u64 xxhash
u24 inode_size_excluding_xxhash
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
union {
  u8[] inline_data
} | {
  u24 tile
  u64 xxhash_of_contents
} | {
  u24 microtile
  u24 microtile_offset
  u64 xxhash_of_contents
} last_tile

**/

typedef enum {
  INO_STATE_OK = 1,
  INO_STATE_CORRUPT = 2,
} ino_state_t;

#define INO_LAST_TILE_THRESHOLD 255

typedef enum {
  // We choose to inline depending on the compute and storage overhead of using a microtile. Currently this threshold is INO_LAST_TILE_THRESHOLD bytes or less (including 0).
  INO_LAST_TILE_MODE_INLINE = 0,
  // We store in a tile if the last tile data has size (TILE_SIZE - INO_LAST_TILE_THRESHOLD) or greater.
  INO_LAST_TILE_MODE_TILE = 1,
  // If there is no existing microtile with enough space, we will create one.
  INO_LAST_TILE_MODE_MICROTILE = 2,
} ino_last_tile_mode_t;
