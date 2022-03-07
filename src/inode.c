#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include "inode.h"
#include "log.h"
#include "util.h"

LOGGER("inode");

struct inodes_state_s {
  // For simplicity, we malloc() one by one, which has an initial cost but allows returning pointers directly (using an array and realloc() allows for changing pointers at any time).
  // All free inode_t values are the same, so we don't need for specific FIFO or LIFO characteristics.
  inode_t* next_free_inode_in_pool;
};

inodes_state_t* inodes_state_create() {
  inodes_state_t* state = malloc(sizeof(inodes_state_t));
  state->next_free_inode_in_pool = NULL;
  return state;
}

// We only create inode_t values from the single-thread manager, so we don't need to figure out how to do this atomically.
inode_t* inode_create_thread_unsafe(
  inodes_state_t* inodes,
  inode_t* next,
  ino_state_t state,
  uint64_t dev_offset
) {
  inode_t* ino;
  if (inodes->next_free_inode_in_pool == NULL) {
    ino = malloc(sizeof(inode_t));

    atomic_init(&ino->next, next);
    atomic_init(&ino->state, state);
    atomic_init(&ino->refcount, 0);
  } else {
    ino = inodes->next_free_inode_in_pool;
    inodes->next_free_inode_in_pool = ino->next_free_inode_in_pool;

    atomic_store_explicit(&ino->next, next, memory_order_relaxed);
    atomic_store_explicit(&ino->state, state, memory_order_relaxed);
    // Do NOT reset refcount in case there are actual users of this released inode.
  }
  atomic_store_explicit(&ino->dev_offset, dev_offset, memory_order_relaxed);
  // For sanity checks against double-free.
  ino->next_free_inode_in_pool = NULL;
  return ino;
}

// We only destroy/release inode_t values from the single-thread manager, so we don't need to figure out how to do this atomically.
void inode_destroy_thread_unsafe(inodes_state_t* inodes, inode_t* ino) {
  // TODO This doesn't handle all possible cases (e.g. pool only has one element).
  DEBUG_ASSERT_STATE(ino->next_free_inode_in_pool == NULL, "inode double free");
  ino->next_free_inode_in_pool = inodes->next_free_inode_in_pool;
  inodes->next_free_inode_in_pool = ino;
}
