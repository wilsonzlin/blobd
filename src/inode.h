#pragma once

/**

INODE
=====

We use a null terminator after the key so that we can use it directly from mmap'ed memory without having to allocate memory, copy the key, and then add a null terminator. This is useful for printing, khash sets, and other libraries and APIs that except C-style strings.

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
u8 key_null_terminator
u24[] tiles
u8[] last_tile_data_if_mode_is_inline

**/

#define INO_OFFSETOF_INODE_SIZE 0
#define INO_OFFSETOF_NEXT_INODE_TILE (INO_OFFSETOF_INODE_SIZE + 3)
#define INO_OFFSETOF_NEXT_INODE_TILE_OFFSET (INO_OFFSETOF_NEXT_INODE_TILE + 3)
#define INO_OFFSETOF_OBJ_NO (INO_OFFSETOF_NEXT_INODE_TILE_OFFSET + 3)
#define INO_OFFSETOF_STATE (INO_OFFSETOF_OBJ_NO + 8)
#define INO_OFFSETOF_SIZE (INO_OFFSETOF_STATE + 1)
#define INO_OFFSETOF_LAST_TILE_MODE (INO_OFFSETOF_SIZE + 5)
#define INO_OFFSETOF_KEY_LEN (INO_OFFSETOF_LAST_TILE_MODE + 1)
#define INO_OFFSETOF_KEY (INO_OFFSETOF_KEY_LEN + 1)
#define INO_OFFSETOF_KEY_NULL_TERM(key_len) (INO_OFFSETOF_KEY + (key_len))
#define INO_OFFSETOF_TILE_NO(key_len, tile) ((INO_OFFSETOF_KEY_NULL_TERM(key_len)) + 1 + 3 * (tile))
#define INO_OFFSETOF_TILES(key_len) (INO_OFFSETOF_TILE_NO(key_len, 0))
#define INO_OFFSETOF_LAST_TILE_INLINE_DATA(key_len, full_tile_count) (INO_OFFSETOF_TILE_NO(key_len, full_tile_count))

typedef enum {
  INO_STATE_INCOMPLETE = 1 << 0,
  INO_STATE_READY = 1 << 1,
  INO_STATE_DELETED = 1 << 2,
} ino_state_t;

typedef enum {
  INO_LAST_TILE_MODE_INLINE = 0,
  INO_LAST_TILE_MODE_TILE = 1,
} ino_last_tile_mode_t;

typedef struct {
  _Atomic(inode_t*) next;
  _Atomic(ino_state_t) state;
  _Atomic(uint64_t) refcount;
  uint32_t tile;
  uint32_t tile_offset;
  // For inode.c internal use only.
  inode_t* next_free_inode_in_pool;
} inode_t;

typedef struct inodes_state_s inodes_state_t;

inodes_state_t* inodes_state_create();

inode_t* inode_create_thread_unsafe(
  inodes_state_t* inodes,
  inode_t* next,
  ino_state_t state,
  uint32_t tile,
  uint32_t tile_offset
);

void inode_destroy_thread_unsafe(inodes_state_t* inodes, inode_t* ino);
