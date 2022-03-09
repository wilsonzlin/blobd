#pragma once

/**

INODE
=====

We use a null terminator after the key so that we can use it directly from mmap'ed memory without having to allocate memory, copy the key, and then add a null terminator. This is useful for printing, khash sets, and other libraries and APIs that except C-style strings.

Structure
---------

The ordering is important, as we want to avoid multiple read() syscalls. Therefore we position `size`, `obj_no`, `key_len`, and `key` together, so it's possible to get all that data in one call, and optionally without `size`.

u24 inode_size
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

#define INO_OFFSETOF_INODE_SIZE 0
#define INO_OFFSETOF_NEXT_INODE_DEV_OFFSET (INO_OFFSETOF_INODE_SIZE + 3)
#define INO_OFFSETOF_STATE (INO_OFFSETOF_NEXT_INODE_DEV_OFFSET + 3)
#define INO_OFFSETOF_LAST_TILE_MODE (INO_OFFSETOF_STATE + 1)
#define INO_OFFSETOF_SIZE (INO_OFFSETOF_LAST_TILE_MODE + 1)
#define INO_OFFSETOF_OBJ_NO (INO_OFFSETOF_SIZE + 5)
#define INO_OFFSETOF_KEY_LEN (INO_OFFSETOF_OBJ_NO + 8)
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

#define INODE_STATE_IS_VALID(x) ((x) == INO_STATE_INCOMPLETE || (x) == INO_STATE_READY || (x) == INO_STATE_DELETED)

typedef enum {
  INO_LAST_TILE_MODE_INLINE = 0,
  INO_LAST_TILE_MODE_TILE = 1,
} ino_last_tile_mode_t;

struct inode_s {
  _Atomic(struct inode_s*) next;
  _Atomic(ino_state_t) state;
  _Atomic(uint64_t) refcount;
  // This still needs to be atomic, even if we read the other fields atomically and with memory barriers:
  // - It's still allowed for an inode to be released then immediately reused, overwriting this field, causing readers to misread.
  _Atomic(uint64_t) dev_offset;
  // For inode.c internal use only.
  struct inode_s* next_free_inode_in_pool;
};

typedef struct inode_s inode_t;

#define INODE_DEV_OFFSET(bkt_ino) atomic_load_explicit(&(bkt_ino)->dev_offset, memory_order_relaxed)

#define INODE_CUR(dev, bkt_ino) ((dev)->mmap + (INODE_DEV_OFFSET(bkt_ino)))

typedef struct inodes_state_s inodes_state_t;

inodes_state_t* inodes_state_create();

inode_t* inode_create_thread_unsafe(
  inodes_state_t* inodes,
  inode_t* next,
  ino_state_t state,
  uint64_t dev_offset
);

void inode_destroy_thread_unsafe(inodes_state_t* inodes, inode_t* ino);
