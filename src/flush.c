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
#include "tile.h"
#include "vec.h"
#include "../ext/xxHash/xxhash.h"

flush_state_t* flush_create() {
  flush_state_t* flush = malloc(sizeof(flush_state_t));
  if (pthread_rwlock_init(&flush->rwlock, NULL)) {
    perror("Failed to create flushing lock");
    exit(EXIT_INTERNAL);
  }
  return flush;
}

typedef struct {
  size_t device_offset;
  uint32_t len;
  uint32_t offset_in_change_data_pool;
} change_t;

LIST_DEF(changes, change_t);
LIST(changes, change_t);

typedef struct {
  flush_state_t* flush;
  svr_clients_t* svr;
  device_t* dev;
  journal_t* journal;
  freelist_t* fl;
  buckets_t* buckets;
  uint32_t* client_ids_buf;
  size_t client_ids_cap;
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
  if (layer == bkts->dirty_sixteen_pointers_layer_count - 1) {
    for (size_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      size_t offset = base * 64 + i1;
      size_t len = 6 * 16 + 8;
      size_t dev_offset = bkts->dev_offset_pointers + (offset * len);
      ensure_change_data_pool_cap(state, *change_data_next + len);
      cursor_t* start = state->change_data_pool + *change_data_next;
      cursor_t* cur = start;
      for (size_t j = 0; j < 16; j++) {
        uint_least64_t v = state->buckets->bucket_pointers[offset * 16 + j];
        uint32_t microtile_offset = v & ((1 << 24) - 1);
        uint32_t microtile = (v >> 24);
        produce_u24(&cur, microtile);
        produce_u24(&cur, microtile_offset);
      }
      uint64_t checksum = XXH3_64bits(start, len - 8);
      produce_u64(&cur, checksum);
      append_change(state->changes, change_data_next, dev_offset, len);
    }
  } else {
    for (size_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      size_t offset = base * 64 + i1;
      visit_bucket_dirty_bitmap(state, change_data_next, bkts->dirty_sixteen_pointers[layer + 1][i1], offset, layer + 1);
    }
  }
}

void* thread(void* state_raw) {
  thread_state_t* state = (thread_state_t*) state_raw;

  while (true) {
    struct timespec sleep_req;
    sleep_req.tv_sec = 0;
    sleep_req.tv_nsec = 100 * 1000 * 1000;
    if (-1 == nanosleep(&sleep_req, NULL)) {
      perror("Failed to sleep flushing worker");
      exit(EXIT_INTERNAL);
    }

    server_clients_acquire_awaiting_flush_lock(state->svr);
    size_t client_len = server_clients_get_awaiting_flush_count(state->svr);
    if (client_len >= state->client_ids_cap) {
      while (client_len >= state->client_ids_cap) {
        state->client_ids_cap *= 2;
      }
      free(state->client_ids_buf);
      state->client_ids_buf = malloc(sizeof(uint32_t) * state->client_ids_cap);
    }
    server_clients_pop_all_awaiting_flush(state->svr, state->client_ids_buf);
    server_clients_release_awaiting_flush_lock(state->svr);

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

    if (state->fl->dirty_eight_tiles_bitmap_1) {
      vec_128i_u8_t i1_candidates = vec_find_indices_of_nonzero_bits_16(state->fl->dirty_eight_tiles_bitmap_1);
      for (size_t o1 = 0, i1; (i1 = i1_candidates.elems[o1]) != 16; o1++) {
        vec_512i_u8_t i2_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_eight_tiles_bitmap_2[i1]);
        for (size_t o2 = 0, i2; (i2 = i2_candidates.elems[o2]) != 64; o2++) {
          vec_512i_u8_t i3_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_eight_tiles_bitmap_3[i1 * 8 + i2]);
          for (size_t o3 = 0, i3; (i3 = i3_candidates.elems[o3]) != 64; o3++) {
            vec_512i_u8_t i4_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_eight_tiles_bitmap_4[(((i1 * 8) + i2) * 64) + i3]);
            for (size_t o4 = 0, i4; (i4 = i4_candidates.elems[o4]) != 64; o4++) {
              uint32_t eight_tiles = ((((i1 * 8) + i2) * 64) + i3) * 64 + i4;
              size_t len = 8 + 1 + 3 * 8;
              size_t dev_offset = state->fl->dev_offset + eight_tiles * len;

              ensure_change_data_pool_cap(state, change_data_next + len);
              cursor_t* start = state->change_data_pool + change_data_next;
              cursor_t* cur = start;
              produce_u8(&cur, (state->fl->tile_bitmap_4[i1][i2][i3] >> (i4 * 8)) & 0xff);
              for (size_t k = 0; k < 8; k++) {
                size_t atmp = eight_tiles * 8 + k;
                size_t a1 = atmp % 16; atmp /= 16;
                size_t a2 = atmp % 16; atmp /= 16;
                size_t a3 = atmp % 16; atmp /= 16;
                size_t a4 = atmp % 16; atmp /= 16;
                size_t a5 = atmp % 16; atmp /= 16;
                size_t a6 = atmp % 16;
                produce_u24(&cur, TILE_SIZE - state->fl->microtile_free_map_6[a1][a2][a3][a4][a5].elems[a6] - 1);
              }
              uint64_t checksum = XXH3_64bits(start, len - 8);
              produce_u64(&cur, checksum);
              append_change(state->changes, &change_data_next, dev_offset, len);
            }
          }
        }
      }
    }

    if (state->buckets->dirty_sixteen_pointers[0][0]) {
      visit_bucket_dirty_bitmap(state, &change_data_next, state->buckets->dirty_sixteen_pointers[0][0], 0, 0);
    }

    // Write and flush journal.
    cursor_t* journal_mmap = state->dev->mmap + state->journal->dev_offset;
    cursor_t* journal_cur = journal_mmap + 8;
    produce_u32(&journal_cur, state->changes->len);
    for (size_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      produce_u48(&journal_cur, c.device_offset);
      produce_u32(&journal_cur, c.len);
      produce_n(&journal_cur, state->dev->mmap + c.device_offset, c.len);
    }
    uint64_t journal_checksum = XXH3_64bits(journal_mmap + 8, journal_cur - (journal_mmap + 8));
    write_u64(journal_mmap, journal_checksum);
    // Ensure journal has been flushed to disk and written successfully.
    msync(state->dev->mmap, state->dev->size, MS_SYNC);

    // Write changes to mmap and flush.
    for (size_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      memcpy(state->dev->mmap + c.device_offset, state->change_data_pool + c.offset_in_change_data_pool, c.len);
    }
    // We must ensure changes have been written successfully BEFORE erasing journal.
    msync(state->dev->mmap, state->dev->size, MS_SYNC);
    state->changes->len = 0;

    // Erase and flush journal.
    journal_cur = journal_mmap;
    produce_u64(&journal_cur, state->xxhash_u32_0);
    produce_u32(&journal_cur, 0);
    // We must flush, or else we'll try recovering from journal and overwrite data.
    msync(state->dev->mmap, state->dev->size, MS_SYNC);

    // Clear dirty bitmaps.
    state->fl->dirty_eight_tiles_bitmap_1 = 0;
    memset(state->fl->dirty_eight_tiles_bitmap_2, 0, 64 * sizeof(uint64_t));
    memset(state->fl->dirty_eight_tiles_bitmap_3, 0, 64 * 64 * sizeof(uint64_t));
    memset(state->fl->dirty_eight_tiles_bitmap_4, 0, 64 * 64 * 8 * sizeof(uint64_t));
    for (size_t i = 0, l = 1; i < state->buckets->dirty_sixteen_pointers_layer_count; i++, l *= 64) {
      memset(state->buckets->dirty_sixteen_pointers[i], 0, l * sizeof(uint64_t));
    }

    if (pthread_rwlock_unlock(&state->flush->rwlock)) {
      perror("Failed to release write lock on flushing");
      exit(EXIT_INTERNAL);
    }

    server_clients_push_all_ready(state->svr, state->client_ids_buf, client_len);
  }
}

void flush_worker_start(
  flush_state_t* flush,
  svr_clients_t* svr,
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  buckets_t* buckets
) {
  thread_state_t* state = malloc(sizeof(thread_state_t));
  state->flush = flush;
  state->svr = svr;
  state->dev = dev;
  state->journal = journal;
  state->fl = fl;
  state->buckets = buckets;
  state->client_ids_cap = server_clients_get_capacity(svr);
  state->client_ids_buf = malloc(sizeof(uint32_t) * state->client_ids_cap);
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
