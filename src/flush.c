#define _GNU_SOURCE

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include "bucket.h"
#include "cursor.h"
#include "device.h"
#include "exit.h"
#include "flush.h"
#include "freelist.h"
#include "journal.h"
#include "list.h"
#include "log.h"
#include "server.h"
#include "stream.h"
#include "tile.h"
#include "vec.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("flush");

typedef struct {
  size_t device_offset;
  uint32_t len;
  uint32_t offset_in_change_data_pool;
} change_t;

LIST_DEF(changes, change_t);
LIST(changes, change_t);

typedef struct {
  flush_state_t* flush;
  server_t* svr;
  device_t* dev;
  journal_t* journal;
  freelist_t* fl;
  buckets_t* buckets;
  stream_t* stream;
  changes_t* changes;
  uint8_t* change_data_pool;
  size_t change_data_pool_cap;
  uint64_t xxhash_u32_0;
} thread_state_t;

static inline void ensure_change_data_pool_cap(thread_state_t* state, size_t cap) {
  if (cap >= state->change_data_pool_cap) {
    while (state->change_data_pool_cap < cap) {
      state->change_data_pool_cap *= 2;
    }
    state->change_data_pool = realloc(state->change_data_pool, state->change_data_pool_cap);
  }
}

// TODO Assert total bytes does not exceed journal space.
static inline void append_change(
  changes_t* changes,
  size_t* change_data_next,
  size_t device_offset,
  uint32_t len
) {
  change_t* last = changes_last_mut(changes);
  if (last != NULL && last->device_offset + last->len == device_offset) {
    // TODO Assert this doesn't overflow.
    last->len += len;
  } else {
    change_t c = {
      .device_offset = device_offset,
      .len = len,
      .offset_in_change_data_pool = *change_data_next,
    };
    changes_append(changes, c);
  }
  *change_data_next += len;
}

void visit_bucket_dirty_bitmap(
  thread_state_t* state,
  size_t* change_data_next,
  uint64_t bitmap,
  size_t base,
  size_t layer
) {
  buckets_t* bkts = state->buckets;
  vec_512i_u8_t candidates = vec_find_indices_of_nonzero_bits_64(bitmap);
  if (layer == bkts->dirty_pointers_layer_count - 1) {
    for (size_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      size_t bkt_id = base + i1;
      size_t len = 6;
      size_t dev_offset = bkts->dev_offset_pointers + (bkt_id * len);
      ensure_change_data_pool_cap(state, *change_data_next + len);
      cursor_t* cur = state->change_data_pool + *change_data_next;
      bucket_t* v = &state->buckets->buckets[bkt_id];
      produce_u24(&cur, v->microtile);
      produce_u24(&cur, v->microtile_byte_offset);
      append_change(state->changes, change_data_next, dev_offset, len);
    }
  } else {
    for (size_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      size_t offset = base + i1;
      visit_bucket_dirty_bitmap(state, change_data_next, bkts->dirty_pointers[layer + 1][offset], offset * 64, layer + 1);
    }
  }
}

void* thread(void* state_raw) {
  thread_state_t* state = (thread_state_t*) state_raw;
  ts_log(DEBUG, "Started flush worker");

  while (true) {
    struct timespec sleep_req;
    sleep_req.tv_sec = 0;
    sleep_req.tv_nsec = 100 * 1000 * 1000;
    if (-1 == nanosleep(&sleep_req, NULL)) {
      perror("Failed to sleep flushing worker");
      exit(EXIT_INTERNAL);
    }

    if (!server_on_flush_start(state->svr)) {
      continue;
    }

    ts_log(DEBUG, "Starting flush");
    if (pthread_rwlock_wrlock(&state->flush->rwlock)) {
      perror("Failed to acquire write lock on flushing");
      exit(EXIT_INTERNAL);
    }

    // We acquire a flushing lock first to ensure all inodes have been completely written to mmap with valid "next" and "hash" field values.
    msync(state->dev->mmap, state->dev->size, MS_SYNC);

    // We collect changes to make first, and then write to journal. This allows two optimisations:
    // - Avoiding paging in the journal until we need to.
    // - Compacting contiguous change list entries, before committing final list to journal.
    // TODO Should we validate existing checksums on device when reading to store into journal? It's most likely cached in memory, so we wouldn't actually be checking for device corruption.

    size_t change_data_next = 0;

    if (state->fl->dirty_tiles_bitmap_1) {
      vec_512i_u8_t i1_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_1);
      for (size_t o1 = 0, i1; (i1 = i1_candidates.elems[o1]) != 64; o1++) {
        vec_512i_u8_t i2_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_2[i1]);
        for (size_t o2 = 0, i2; (i2 = i2_candidates.elems[o2]) != 64; o2++) {
          vec_512i_u8_t i3_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_3[i1 * 64 + i2]);
          for (size_t o3 = 0, i3; (i3 = i3_candidates.elems[o3]) != 64; o3++) {
            vec_512i_u8_t i4_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_4[(((i1 * 64) + i2) * 64) + i3]);
            for (size_t o4 = 0, i4; (i4 = i4_candidates.elems[o4]) != 64; o4++) {
              size_t tile_no = ((((i1 * 64) + i2) * 64) + i3) * 64 + i4;
              size_t len = 3;
              size_t dev_offset = state->fl->dev_offset + tile_no * len;

              ensure_change_data_pool_cap(state, change_data_next + len);
              cursor_t* start = state->change_data_pool + change_data_next;
              cursor_t* cur = start;

              size_t atmp = tile_no;
              size_t a6 = atmp % 16; atmp /= 16;
              size_t a5 = atmp % 16; atmp /= 16;
              size_t a4 = atmp % 16; atmp /= 16;
              size_t a3 = atmp % 16; atmp /= 16;
              size_t a2 = atmp % 16; atmp /= 16;
              size_t a1 = atmp % 16;

              uint32_t microtile_state = state->fl->microtile_free_map_6[a1][a2][a3][a4][a5].elems[a6];
              if (microtile_state == 16) {
                // This tile is NOT a microtile.
                bool is_free = state->fl->tile_bitmap_4[i1][i2][i3] & (1llu << i4);
                if (is_free) {
                  produce_u24(&cur, 16777215);
                } else {
                  produce_u24(&cur, 16777214);
                }
              } else {
                // This tile is a microtile.
                uint32_t free = microtile_state >> 8;
                // Avoid invalid special values. Note that microtiles cannot have usages of 0, 1, or 2 bytes, because they must have at least one inode which are at least 29 bytes.
                if (free >= 16777214) {
                  fprintf(stderr, "Microtile %zu has free space of %u bytes\n", atmp, free);
                  exit(EXIT_INTERNAL);
                }
                produce_u24(&cur, free);
              }

              append_change(state->changes, &change_data_next, dev_offset, len);
            }
          }
        }
      }
    }

    if (state->buckets->dirty_pointers[0][0]) {
      ts_log(DEBUG, "Bucket pointers have changed");
      visit_bucket_dirty_bitmap(state, &change_data_next, state->buckets->dirty_pointers[0][0], 0, 0);
    }

    if (state->stream->pending_flush->len) {
      ensure_change_data_pool_cap(state, 8 + state->stream->pending_flush->len * (1 + 8 + 8));
      cursor_t* cur = state->change_data_pool + change_data_next;
      produce_u64(&cur, atomic_load_explicit(&state->stream->next_obj_no, memory_order_relaxed));
      produce_u64(&cur, atomic_load_explicit(&state->stream->next_seq_no, memory_order_relaxed));
      append_change(state->changes, &change_data_next, state->stream->dev_offset, 8);
      for (size_t i = 0; i < state->stream->pending_flush->len; i++) {
        stream_event_t* ev = state->stream->pending_flush->elems + i;
        uint64_t ring_idx = ev->seq_no % STREAM_EVENTS_BUF_LEN;
        produce_u8(&cur, ev->typ);
        produce_u40(&cur, ev->bkt_id);
        produce_u64(&cur, ev->obj_no);
        append_change(
          state->changes,
          &change_data_next,
          state->stream->dev_offset + 8 + (ring_idx * (1 + 8 + 8)),
          1 + 8 + 8
        );
      }
      state->stream->pending_flush->len = 0;
    }

    // Write and flush journal.
    for (size_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      journal_append(state->journal, c.device_offset, c.len);
    }
    // Ensure journal has been flushed to disk and written successfully.
    journal_flush(state->journal);

    // Write changes to mmap and flush.
    for (size_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      memcpy(state->dev->mmap + c.device_offset, state->change_data_pool + c.offset_in_change_data_pool, c.len);
    }
    // We must ensure changes have been written successfully BEFORE erasing journal.
    msync(state->dev->mmap, state->dev->size, MS_SYNC);
    state->changes->len = 0;

    // Erase and flush journal.
    journal_clear(state->journal);

    // Clear dirty bitmaps.
    state->fl->dirty_tiles_bitmap_1 = 0;
    memset(state->fl->dirty_tiles_bitmap_2, 0, 64 * sizeof(uint64_t));
    memset(state->fl->dirty_tiles_bitmap_3, 0, 64 * 64 * sizeof(uint64_t));
    memset(state->fl->dirty_tiles_bitmap_4, 0, 64 * 64 * 64 * sizeof(uint64_t));
    for (size_t i = 0, l = 1; i < state->buckets->dirty_pointers_layer_count; i++, l *= 64) {
      memset(state->buckets->dirty_pointers[i], 0, l * sizeof(uint64_t));
    }

    if (pthread_rwlock_unlock(&state->flush->rwlock)) {
      perror("Failed to release write lock on flushing");
      exit(EXIT_INTERNAL);
    }

    server_on_flush_end(state->svr);
    ts_log(DEBUG, "Flush ended");
  }
}

void flush_worker_start(
  flush_state_t* flush,
  server_t* svr,
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  buckets_t* buckets,
  stream_t* stream
) {
  thread_state_t* state = malloc(sizeof(thread_state_t));
  state->flush = flush;
  state->svr = svr;
  state->dev = dev;
  state->journal = journal;
  state->fl = fl;
  state->buckets = buckets;
  state->stream = stream;
  state->changes = changes_create_with_capacity(128);
  state->change_data_pool_cap = 2048;
  state->change_data_pool = malloc(state->change_data_pool_cap);
  uint8_t u32_0[4];
  write_u32(u32_0, 0);
  state->xxhash_u32_0 = XXH3_64bits(u32_0, 4);

  pthread_t t;
  if (pthread_create(&t, NULL, thread, state)) {
    perror("Failed to start flush worker thread");
    exit(EXIT_INTERNAL);
  }
}
