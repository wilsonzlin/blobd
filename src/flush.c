#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include "../ext/xxHash/xxhash.h"
#include "bucket.h"
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "flush.h"
#include "freelist.h"
#include "inode.h"
#include "journal.h"
#include "list.h"
#include "log.h"
#include "stream.h"
#include "tile.h"
#include "util.h"
#include "vec.h"

LOGGER("flush");

#define FUTURE_FLUSH_CLIENTS_CAP_INIT 32
#define FUTURE_FLUSH_DELETES_CAP_INIT 32
#define FUTURE_FLUSH_WRITE_BYTES_THRESHOLD (8 * 1024 * 1024)
// Assuming 300,000 IOPS, we can do 30,000 in 100 ms, so don't wait for next 100 ms tick if we've already got that many pending operations.
// TODO Tune, verify theory.
#define FUTURE_FLUSH_WRITE_COUNT_THRESHOLD 30000

LIST_DEF(clients, svr_client_t*);
LIST(clients, svr_client_t*);

LIST_DEF(inode_dev_offsets, uint64_t);
LIST(inode_dev_offsets, uint64_t);

typedef struct future_flush_s {
  uint8_t journal[JOURNAL_RESERVED_SPACE];
  uint32_t journal_entry_count;
  uint32_t journal_write_next;
  // Some methods have lots of data for their journal entry, but have strict ordering requirements, so to avoid forcing them to hold a lock for a long time, they can reserve the space and position in the journal first and then inform us when they're done populating the journal data.
  uint32_t journal_pending_count;
  // Set to true when the next journal event is on another future_flush_t. Since flush tasks/journal entries are strictly ordered, nothing more will be added to this future_flush_t, so it's ready to be flushed.
  bool journal_full;
  // Optimisation: inodes to delete are copied separately for faster iterating.
  inode_dev_offsets_t* delete_inode_dev_offsets;
  // Optimisation: clients currently in write_object can be released earlier, before the journal is applied.
  clients_t* nonwrite_clients;
  clients_t* write_clients;
  // Count of clients that are in write_object and pending flush.
  uint64_t write_count;
  // Sum of bytes written by clients in write_object pending flush.
  uint64_t write_bytes;
  // Internal use only.
  bool ready;
  // When in free pool, this points to next free future_flush_t. Otherwise, it points to next future_flush_t to flush once ready.
  struct future_flush_s* next;
} future_flush_t;

struct flush_state_s {
  buckets_t* buckets;
  device_t* dev;
  freelist_t* freelist;
  journal_t* journal;
  server_t* server;
  stream_t* stream;

  // We can't use svr_client_t directly as a commit_object could have multiple tasks (e.g. commit and delete).
  // Atomics generally ruin performance, especially with multiple operations. A CPU can run billions of cycles per second but we only expect at most 100K creates/commits/writes/deletes per second, so using a lock would not be considered contentious and atomics would therefore be net negative for performance. Additional reasons we don't use them here:
  // - A threshold for performing flush is the amount of tasks. If multiple threads realise there are enough tasks, they'll all call flush_maybe_perform, causing contention on the flushing_lock and wasted compute.
  // - We'd have to use a ring buffer since there's no way to atomically get an index, check the position, conditionally swap task array buffers, get actual readable length, and call flush_maybe_perform.
  // - With a ring buffer, we'd run the risk of overwriting (once again, no atomic way of checking all variables atomically). We'd also have to allocate a huge buffer upfront as we cannot realloc().
  pthread_mutex_t futures_lock;
  pthread_cond_t futures_cond;
  future_flush_t* future_head;
  future_flush_t* future_next_writable;

  // The pool is a stack instead of a queue, so we don't need a tail.
  future_flush_t* future_pool_head;
};

static inline void reset_future(future_flush_t* f) {
  f->journal_entry_count = 0;
  f->journal_write_next = JOURNAL_OFFSETOF_ENTRIES;
  f->journal_pending_count = 0;
  f->journal_full = false;
  f->delete_inode_dev_offsets->len = 0;
  f->nonwrite_clients->len = 0;
  f->write_clients->len = 0;
  f->write_count = 0;
  f->write_bytes = 0;
  f->ready = false;
  f->next = NULL;
}

static inline future_flush_t* create_future() {
  future_flush_t* f = malloc(sizeof(future_flush_t));
  f->delete_inode_dev_offsets = inode_dev_offsets_create_with_capacity(FUTURE_FLUSH_DELETES_CAP_INIT);
  f->nonwrite_clients = clients_create_with_capacity(FUTURE_FLUSH_CLIENTS_CAP_INIT);
  f->write_clients = clients_create_with_capacity(FUTURE_FLUSH_CLIENTS_CAP_INIT);
  reset_future(f);
  return f;
}

// Must be locked before calling.
static inline future_flush_t* get_new_future(flush_state_t* state) {
  future_flush_t* f = state->future_pool_head;
  if (f != NULL) {
    state->future_pool_head = f->next;
    return f;
  }
  return create_future();
}

void flush_lock_tasks(flush_state_t* state) {
  ASSERT_ERROR_RETVAL_OK(pthread_mutex_lock(&state->futures_lock), "lock futures");
}

void flush_unlock_tasks(flush_state_t* state) {
  ASSERT_ERROR_RETVAL_OK(pthread_mutex_unlock(&state->futures_lock), "unlock futures");
}

// Must be locked before calling.
static inline void maybe_apply_future(flush_state_t* state, future_flush_t* f) {
  if (
    !f->journal_pending_count && (
      f->journal_full ||
      f->write_bytes > FUTURE_FLUSH_WRITE_BYTES_THRESHOLD ||
      f->write_count > FUTURE_FLUSH_WRITE_COUNT_THRESHOLD
    )
  ) {
    f->ready = true;
    // We can only flush in order, so we can only flush this if it's the next in line.
    if (f == state->future_head) {
      ASSERT_ERROR_RETVAL_OK(pthread_cond_signal(&state->futures_cond), "signal cond");
    }
  }
}

// Must be locked before calling.
// NOTE: Use either (flush_reserve_task() then flush_commit_task()) or flush_add_write_task(), not both.
flush_task_reserve_t flush_reserve_task(flush_state_t* state, uint32_t len, svr_client_t* client_or_null, uint64_t delete_inode_dev_offset_or_zero) {
  future_flush_t* f = state->future_next_writable;
  if (f->journal_write_next + len > JOURNAL_RESERVED_SPACE) {
    f->journal_full = true;
    future_flush_t* new_f = get_new_future(state);
    f->next = new_f;
    maybe_apply_future(state, f);
    state->future_next_writable = f;
    f = new_f;
  }
  uint32_t pos = f->journal_write_next;
  f->journal_entry_count++;
  f->journal_write_next += len;
  f->journal_pending_count++;
  if (client_or_null != NULL) {
    clients_append(f->nonwrite_clients, client_or_null);
  }
  if (delete_inode_dev_offset_or_zero) {
    inode_dev_offsets_append(f->delete_inode_dev_offsets, delete_inode_dev_offset_or_zero);
  }
  flush_task_reserve_t r = {
    .future = f,
    .pos = pos,
  };
  return r;
}

cursor_t* flush_get_reserved_cursor(flush_task_reserve_t r) {
  future_flush_t* fut = (future_flush_t*) r.future;
  return fut->journal + r.pos;
}

// Must be locked before calling.
// NOTE: Use either (flush_reserve_task() then flush_commit_task()) or flush_add_write_task(), not both.
// WARNING: This may start the flush process, so make sure any client states are updated before calling, as they will be rearmed to the server epoll.
void flush_commit_task(flush_state_t* state, flush_task_reserve_t r) {
  future_flush_t* fut = (future_flush_t*) r.future;
  fut->journal_pending_count--;
  maybe_apply_future(state, fut);
}

// Must be locked before calling.
// NOTE: Use either (flush_reserve_task() then flush_commit_task()) or flush_add_write_task(), not both.
// WARNING: This may start the flush process, so make sure any client states are updated before calling, as they will be rearmed to the server epoll.
void flush_add_write_task(flush_state_t* state, svr_client_t* client, uint64_t write_bytes) {
  future_flush_t* f = state->future_next_writable;
  f->write_count++;
  f->write_bytes += write_bytes;
  clients_append(f->write_clients, client);
  maybe_apply_future(state, f);
}

flush_state_t* flush_state_create(
  buckets_t* buckets,
  device_t* dev,
  freelist_t* freelist,
  journal_t* journal,
  server_t* server,
  stream_t* stream
) {
  flush_state_t* state = malloc(sizeof(flush_state_t));
  state->dev = dev;
  state->journal = journal;
  state->freelist = freelist;
  state->buckets = buckets;
  state->server = server;
  state->stream = stream;

  pthread_mutex_init(&state->futures_lock, NULL);
  pthread_condattr_t condattr;
  ASSERT_ERROR_RETVAL_OK(pthread_condattr_init(&condattr), "init condattr");
  ASSERT_ERROR_RETVAL_OK(pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC), "set condattr clock");
  ASSERT_ERROR_RETVAL_OK(pthread_cond_init(&state->futures_cond, &condattr), "init cond");
  ASSERT_ERROR_RETVAL_OK(pthread_condattr_destroy(&condattr), "destroy condattr");
  state->future_head = state->future_next_writable = create_future();

  state->future_pool_head = NULL;
  return state;
}

void* thread(void* state_raw) {
  flush_state_t* state = (flush_state_t*) state_raw;
  future_flush_t* fut = NULL;
  while (true) {
    ASSERT_ERROR_RETVAL_OK(pthread_mutex_lock(&state->futures_lock), "lock futures");

    // If `fut` is not NULL, it came from a previous iteration and we need to release it back to the pool.
    if (fut != NULL) {
      if (state->future_pool_head == NULL) {
        state->future_pool_head = NULL;
      } else {
        state->future_pool_head->next = fut;
      }
    }

    while ((fut = state->future_head) == NULL || !fut->ready) {
      // Get timestamp inside loop, since significant time can pass between iterations.
      struct timespec wakets;
      if (-1 == clock_gettime(CLOCK_MONOTONIC, &wakets)) {
        perror("Failed to get monotonic timestamp");
        exit(EXIT_INTERNAL);
      }
      // Aim to flush at least every 100 ms.
      if ((wakets.tv_nsec += 100000000) >= 1000000000) {
        wakets.tv_sec++;
        wakets.tv_nsec %= 1000000000;
      }
      int condres = pthread_cond_timedwait(&state->futures_cond, &state->futures_lock, &wakets);
      if (ETIMEDOUT == condres) {
        if (fut != NULL && !fut->journal_pending_count && (fut->write_clients->len || fut->nonwrite_clients->len)) {
          // We force-flush the future anyway due to timeout.
          fut->ready = true;
          state->future_head = fut->next;
          break;
        }
      } else {
        ASSERT_ERROR_RETVAL_OK(condres, "wait cond");
      }
    }
    ASSERT_ERROR_RETVAL_OK(pthread_mutex_unlock(&state->futures_lock), "unlock futures");

    DEBUG_TS_LOG("Starting flush");

    if (fut->journal_entry_count) {
      // Write journal.
      write_u32(fut->journal + JOURNAL_OFFSETOF_COUNT, fut->journal_entry_count);
      uint64_t checksum = XXH3_64bits(fut->journal + JOURNAL_OFFSETOF_COUNT, fut->journal_write_next - JOURNAL_OFFSETOF_COUNT);
      write_u64(fut->journal + JOURNAL_OFFSETOF_CHECKSUM, checksum);
      memcpy(state->journal->mmap, fut->journal, fut->journal_write_next);
      // Sync entire device to also sync pending write_object writes.
      device_sync(state->dev, 0, state->dev->size);
    }

    // Optimisation: release write clients earlier than others.
    for (uint64_t i = 0; i < fut->write_clients->len; i++) {
      svr_client_t* client = fut->write_clients->elems[i];
      server_rearm_client_to_epoll(state->server, client, false, true);
    }

    if (fut->journal_entry_count) {
      // Apply changes.
      journal_apply_online_then_clear(state->journal, state->buckets, state->stream);

      // Release deleted space.
      // We release deleted space AFTER journaling, applying, and syncing changes to device as otherwise some create_object call could immediately convert a tile to a microtile and overwrite data.
      freelist_lock(state->freelist);
      for (uint64_t i = 0; i < fut->delete_inode_dev_offsets->len; i++) {
        uint64_t inode_dev_offset = fut->delete_inode_dev_offsets->elems[i];
        freelist_replenish_tiles_of_inode(state->freelist, state->dev->mmap + inode_dev_offset);
      }
      freelist_unlock(state->freelist);
    }

    // Release remaining clients.
    for (uint64_t i = 0; i < fut->nonwrite_clients->len; i++) {
      svr_client_t* client = fut->nonwrite_clients->elems[i];
      server_rearm_client_to_epoll(state->server, client, false, true);
    }

    // Reset future and release to pool.
    reset_future(fut);
    // We'll release to pool in the next iteration, where we'll have the lock.

    DEBUG_TS_LOG("Flush ended");
  }
}

void* flush_worker_start(flush_state_t* state) {
  pthread_t* t = malloc(sizeof(pthread_t));
  ASSERT_ERROR_RETVAL_OK(pthread_create(t, NULL, thread, state), "create flush thread");
  return t;
}

void flush_worker_join(void* handle) {
  pthread_t* t = (pthread_t*) handle;
  ASSERT_ERROR_RETVAL_OK(pthread_join(*t, NULL), "join flush thread");
  free(t);
}
