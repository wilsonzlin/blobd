#define _GNU_SOURCE

#include <errno.h>
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
#include "inode.h"
#include "journal.h"
#include "list.h"
#include "log.h"
#include "server.h"
#include "stream.h"
#include "tile.h"
#include "vec.h"
#include "../ext/xxHash/xxhash.h"
#include "../ext/klib/khash.h"

LOGGER("flush");

typedef struct {
  uint64_t device_offset;
  uint32_t len;
  uint32_t offset_in_change_data_pool;
  uint64_t requires_write_lock_on_bucket_id;
} change_t;

LIST_DEF(changes, change_t);
LIST(changes, change_t);

LIST_DEF(bucket_id_pool, uint64_t);
LIST(bucket_id_pool, uint64_t);

KHASH_SET_INIT_STR(str_set);

typedef struct {
  flush_state_t* flush;
  server_t* svr;
  device_t* dev;
  journal_t* journal;
  freelist_t* fl;
  buckets_t* buckets;
  stream_t* stream;
  bucket_id_pool_t* bucket_id_pool;
  kh_str_set_t* key_pool;
  changes_t* changes;
  uint8_t* change_data_pool;
  uint64_t change_data_pool_cap;
  uint64_t xxhash_u32_0;
} thread_state_t;

typedef struct {
  thread_state_t* state;
  uint64_t change_data_next;
} change_data_pool_writer_t;

// Returns 1 if a new change was appended or 0 if the previous change was simply extended.
// TODO Assert total bytes does not exceed journal space.
static inline uint8_t append_change(
  changes_t* changes,
  uint64_t change_data_offset,
  uint64_t device_offset,
  uint32_t len,
  uint64_t requires_write_lock_on_bucket_id
) {
  change_t* last = changes_last_mut(changes);
  if (last != NULL && last->device_offset + last->len == device_offset && last->offset_in_change_data_pool + last->len == change_data_offset && last->requires_write_lock_on_bucket_id == requires_write_lock_on_bucket_id) {
    // TODO Assert this doesn't overflow.
    last->len += len;
    return 0;
  } else {
    change_t c = {
      .device_offset = device_offset,
      .len = len,
      .offset_in_change_data_pool = change_data_offset,
      .requires_write_lock_on_bucket_id = requires_write_lock_on_bucket_id,
    };
    changes_append(changes, c);
    return 1;
  }
}

static inline void change_data_pool_append(change_data_pool_writer_t* writer, uint8_t* data, uint64_t len) {
  if (writer->change_data_next + len >= writer->state->change_data_pool_cap) {
    while (writer->state->change_data_pool_cap < writer->change_data_next + len) {
      writer->state->change_data_pool_cap *= 2;
    }
    writer->state->change_data_pool = realloc(writer->state->change_data_pool, writer->state->change_data_pool_cap);
  }
  memcpy(writer->state->change_data_pool, data, len);
  writer->change_data_next += len;
}

static inline uint32_t change_data_pool_append_u8(change_data_pool_writer_t* writer, uint8_t val) {
  change_data_pool_append(writer, &val, 1);
  return 1;
}

static inline uint32_t change_data_pool_append_u24(change_data_pool_writer_t* writer, uint32_t val) {
  uint8_t data[3];
  write_u24(data, val);
  change_data_pool_append(writer, data, 3);
  return 3;
}

static inline uint32_t change_data_pool_append_u40(change_data_pool_writer_t* writer, uint64_t val) {
  uint8_t data[5];
  write_u40(data, val);
  change_data_pool_append(writer, data, 5);
  return 5;
}

static inline uint32_t change_data_pool_append_u64(change_data_pool_writer_t* writer, uint64_t val) {
  uint8_t data[8];
  write_u64(data, val);
  change_data_pool_append(writer, data, 8);
  return 8;
}

#define __RECORD_CHANGE(dev_offset, requires_write_lock_on_bucket_id, change_exprs_added) \
  append_change(state->changes, change_data_writer->change_data_next, dev_offset, change_exprs_added, requires_write_lock_on_bucket_id)

#define RECORD_CHANGE1_BUCKET(dev_offset, bkt_id, c1) __RECORD_CHANGE(dev_offset, bkt_id, c1)

#define RECORD_CHANGE1(dev_offset, c1) RECORD_CHANGE1_BUCKET(dev_offset, 0, c1)

#define RECORD_CHANGE2(dev_offset, c1, c2) __RECORD_CHANGE(dev_offset, 0, (c1) + (c2))

#define RECORD_CHANGE3_BUCKET(dev_offset, bkt_id, c1, c2, c3) __RECORD_CHANGE(dev_offset, bkt_id, (c1) + (c2) + (c3))

#define RECORD_CHANGE3(dev_offset, c1, c2, c3) RECORD_CHANGE3_BUCKET(dev_offset, 0, c1, c2, c3)

static inline void record_bucket_pointer_change(
  thread_state_t* state,
  change_data_pool_writer_t* change_data_writer,
  uint64_t bkt_id,
  uint32_t new_tile,
  uint32_t new_tile_offset
) {
  uint64_t dev_offset = buckets_get_device_offset_of_bucket(state->buckets, bkt_id);
  RECORD_CHANGE2(
    dev_offset,
    change_data_pool_append_u24(change_data_writer, new_tile),
    change_data_pool_append_u24(change_data_writer, new_tile_offset)
  );
}

void visit_bucket_dirty_bitmap(
  thread_state_t* state,
  change_data_pool_writer_t* change_data_writer,
  uint64_t bitmap,
  uint64_t base,
  uint8_t layer
) {
  buckets_t* bkts = state->buckets;
  vec_512i_u8_t candidates = vec_find_indices_of_nonzero_bits_64(bitmap);
  if (layer == buckets_get_dirty_bitmap_layer_count(bkts) - 1) {
    for (uint64_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      uint64_t bkt_id = base + i1;
      bucket_t* v = buckets_get_bucket(state->buckets, bkt_id);
      record_bucket_pointer_change(state, change_data_writer, bkt_id, v->tile, v->tile_offset);
    }
  } else {
    for (uint64_t o1 = 0, i1; (i1 = candidates.elems[o1]) != 64; o1++) {
      uint64_t offset = base + i1;
      visit_bucket_dirty_bitmap(state, change_data_writer, buckets_get_dirty_bitmap_layer(bkts, layer + 1)[offset], offset * 64, layer + 1);
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

    change_data_pool_writer_t cdw = {
      .change_data_next = 0,
      .state = state,
    };
    change_data_pool_writer_t* change_data_writer = &cdw;

    // Process buckets with pending deletes or commits.
    buckets_pending_delete_or_commit_lock(state->buckets);
    for (
      uint32_t it = 0;
      it != buckets_pending_delete_or_commit_iterator_end(state->buckets);
      it++
    ) {
      // We append to pool so we can clear pending list and release lock ASAP.
      uint64_t bkt_id = buckets_pending_delete_or_commit_iterator_get(state->buckets, it);
      if (!bkt_id) {
        continue;
      }
      bucket_id_pool_append(state->bucket_id_pool, bkt_id);
    }
    buckets_clear_pending_delete_or_commit(state->buckets);
    buckets_pending_delete_or_commit_unlock(state->buckets);
    for (
      uint64_t i = 0;
      i < state->bucket_id_pool->len;
      i++, kh_clear_str_set(state->key_pool)
    ) {
      uint64_t bkt_id = state->bucket_id_pool->elems[i];
      bucket_t* bkt = buckets_get_bucket(state->buckets, bkt_id);
      // For performance, we only take a read lock here when collecting changes, and then a write lock later to apply them to the mmap. This means we'll need to track how many changes there are for a bucket so we know how long to hold a write lock for later. This is what we use bucket_t->pending_flush_changes for, which allows us to avoid having to create a separate map data structure.
      // At this point, bucket_t->pending_flush_changes should already be zero.
      if (pthread_rwlock_rdlock(&bkt->lock)) {
        perror("Failed to acquire read lock on bucket");
        exit(EXIT_INTERNAL);
      }
      uint64_t head_inode_dev_offset = 0;
      uint64_t previous_inode_dev_offset = 0;
      uint32_t ino_tile = bkt->tile;
      uint32_t ino_tile_offset = bkt->tile_offset;
      uint64_t original_head_inode_dev_offset = (TILE_SIZE * ino_tile) + ino_tile_offset;
      while (ino_tile) {
        uint64_t ino_dev_offset = (TILE_SIZE * ino_tile) + ino_tile_offset;
        cursor_t* cur = state->dev->mmap + ino_dev_offset;
        ino_tile = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE);
        ino_tile_offset = read_u24(cur + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET);
        ino_state_t ino_state = cur[INO_OFFSETOF_STATE];
        uint64_t ino_obj_no = read_u64(cur + INO_OFFSETOF_OBJ_NO);
        bool skip_inode = false;
        if (
          ino_state == INO_STATE_DELETED ||
          (ino_state == INO_STATE_READY && kh_get_str_set(state->key_pool, (char*) (cur + INO_OFFSETOF_KEY)) != kh_end(state->key_pool))
        ) {
          skip_inode = true;
          // We can safely modify the freelist because no create_object methods can be running right now. Also, since no create_object can run during the entire flush, we can be sure the freed tiles won't be immediately used, causing readers to read overwritten data.
          freelist_replenish_tiles_of_inode(state->fl, cur);
          // It doesn't really matter if we do this atomically or not. The only possible transition is to INO_STATE_DELETED, so the only possible race condition is another delete_object method (we currently aren't holding a write lock), which doesn't matter. We do need to change this in case current state is INO_STATE_READY.
          cur[INO_OFFSETOF_STATE] = INO_STATE_DELETED;
          // We don't need strict memory ordering, since we never read the data written to stream events on mmap.
          uint64_t seq_no = atomic_fetch_add_explicit(&state->stream->next_seq_no, 1, memory_order_relaxed);
          // We don't need any locks, as we are the only ones who use this list.
          stream_event_t ev = {
            .bkt_id = bkt_id,
            .obj_no = ino_obj_no,
            .seq_no = seq_no,
            .typ = STREAM_EVENT_OBJECT_DELETE,
          };
          events_pending_flush_append(state->stream->pending_flush, ev);
        } else if (ino_state == INO_STATE_COMMITTED) {
          // Delete other objects with same key.
          // Because newer objects are prepended (not appended) to a bucket's inode list, and newer objects have higher object numbers than older ones, we assume that any existing object with the same key will come further down the list (and we haven't already past them).
          int kh_res;
          kh_put_str_set(state->key_pool, (char*) (cur + INO_OFFSETOF_KEY), &kh_res);
          if (-1 == kh_res) {
            fprintf(stderr, "Failed to insert key into pool\n");
            exit(EXIT_INTERNAL);
          }
          bkt->pending_flush_changes += RECORD_CHANGE1_BUCKET(
            ino_dev_offset + INO_OFFSETOF_STATE,
            bkt_id,
            change_data_pool_append_u8(change_data_writer, INO_STATE_READY)
          );
          // Even though we haven't written the updated state yet to mmap, it will be there by the time this event is written as both are atomically written at the same time as part of this flush.
          uint64_t seq_no = atomic_fetch_add_explicit(&state->stream->next_seq_no, 1, memory_order_acquire);
          // We don't need any locks, as we are the only ones who use this list.
          stream_event_t ev = {
            .bkt_id = bkt_id,
            .obj_no = ino_obj_no,
            .seq_no = seq_no,
            .typ = STREAM_EVENT_OBJECT_COMMIT,
          };
          events_pending_flush_append(state->stream->pending_flush, ev);
        }
        if (!skip_inode) {
          if (previous_inode_dev_offset) {
            bkt->pending_flush_changes += RECORD_CHANGE1_BUCKET(
              previous_inode_dev_offset + INO_OFFSETOF_NEXT_INODE_TILE,
              bkt_id,
              change_data_pool_append_u24(change_data_writer, ino_tile)
            );
            bkt->pending_flush_changes += RECORD_CHANGE1_BUCKET(
              previous_inode_dev_offset + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET,
              bkt_id,
              change_data_pool_append_u24(change_data_writer, ino_tile_offset)
            );
          }
          if (!head_inode_dev_offset) {
            head_inode_dev_offset = ino_dev_offset;
          }
          previous_inode_dev_offset = ino_dev_offset;
        }
      }
      // Update tail inode.
      if (previous_inode_dev_offset) {
        bkt->pending_flush_changes += RECORD_CHANGE1_BUCKET(
          previous_inode_dev_offset + INO_OFFSETOF_NEXT_INODE_TILE,
          bkt_id,
          change_data_pool_append_u24(change_data_writer, 0)
        );
        bkt->pending_flush_changes += RECORD_CHANGE1_BUCKET(
          previous_inode_dev_offset + INO_OFFSETOF_NEXT_INODE_TILE_OFFSET,
          bkt_id,
          change_data_pool_append_u24(change_data_writer, 0)
        );
      }
      if (head_inode_dev_offset != original_head_inode_dev_offset) {
        // NOTE: We don't actually modify the real in-memory bucket pointer, as we haven't committed the changes to the inodes list on the heap yet.
        // TODO This might be duplicated with dirty bucket updates.
        record_bucket_pointer_change(
          state,
          change_data_writer,
          bkt_id,
          original_head_inode_dev_offset / TILE_SIZE,
          original_head_inode_dev_offset % TILE_SIZE
        );
      }
      if (pthread_rwlock_unlock(&bkt->lock)) {
        perror("Failed to release read lock on bucket");
        exit(EXIT_INTERNAL);
      }
    }
    state->bucket_id_pool->len = 0;

    // Record freelist changes.
    // NOTE: Do this after processing object deletes/commits, as those can cause space to be freed.
    if (state->fl->dirty_tiles_bitmap_1) {
      vec_512i_u8_t i1_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_1);
      for (uint64_t o1 = 0, i1; (i1 = i1_candidates.elems[o1]) != 64; o1++) {
        vec_512i_u8_t i2_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_2[i1]);
        for (uint64_t o2 = 0, i2; (i2 = i2_candidates.elems[o2]) != 64; o2++) {
          vec_512i_u8_t i3_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_3[i1 * 64 + i2]);
          for (uint64_t o3 = 0, i3; (i3 = i3_candidates.elems[o3]) != 64; o3++) {
            vec_512i_u8_t i4_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_4[(((i1 * 64) + i2) * 64) + i3]);
            for (uint64_t o4 = 0, i4; (i4 = i4_candidates.elems[o4]) != 64; o4++) {
              uint64_t tile_no = ((((i1 * 64) + i2) * 64) + i3) * 64 + i4;
              uint64_t dev_offset = state->fl->dev_offset + tile_no * 3;

              uint64_t atmp = tile_no;
              uint64_t a6 = atmp % 16; atmp /= 16;
              uint64_t a5 = atmp % 16; atmp /= 16;
              uint64_t a4 = atmp % 16; atmp /= 16;
              uint64_t a3 = atmp % 16; atmp /= 16;
              uint64_t a2 = atmp % 16; atmp /= 16;
              uint64_t a1 = atmp % 16;

              uint32_t new_value;

              uint32_t microtile_state = state->fl->microtile_free_map_6[a1][a2][a3][a4][a5].elems[a6];
              if (microtile_state == 16) {
                // This tile is NOT a microtile.
                bool is_free = state->fl->tile_bitmap_4[i1][i2][i3] & (1llu << i4);
                if (is_free) {
                  new_value = 16777215;
                } else {
                  new_value = 16777214;
                }
              } else {
                // This tile is a microtile.
                uint32_t free = microtile_state >> 8;
                // Avoid invalid special values. Note that microtiles cannot have usages of 0, 1, or 2 bytes, because they must have at least one inode which are at least 29 bytes.
                if (free >= 16777214) {
                  fprintf(stderr, "Microtile %zu has free space of %u bytes\n", atmp, free);
                  exit(EXIT_INTERNAL);
                }
                new_value = free;
              }
              RECORD_CHANGE1(dev_offset, change_data_pool_append_u24(change_data_writer, new_value));
            }
          }
        }
      }
    }

    // Record stream changes.
    // NOTE: Do this after processing object deletes/commits, as those create events.
    if (state->stream->pending_flush->len) {
      RECORD_CHANGE2(
        state->stream->dev_offset,
        change_data_pool_append_u64(change_data_writer, atomic_load_explicit(&state->stream->next_obj_no, memory_order_relaxed)),
        change_data_pool_append_u64(change_data_writer, atomic_load_explicit(&state->stream->next_seq_no, memory_order_relaxed))
      );
      for (uint64_t i = 0; i < state->stream->pending_flush->len; i++) {
        stream_event_t* ev = state->stream->pending_flush->elems + i;
        uint64_t ring_idx = ev->seq_no % STREAM_EVENTS_BUF_LEN;
        RECORD_CHANGE3(
          state->stream->dev_offset + 8 + 8 + (ring_idx * (1 + 5 + 8)),
          change_data_pool_append_u8(change_data_writer, ev->typ),
          change_data_pool_append_u40(change_data_writer, ev->bkt_id),
          change_data_pool_append_u64(change_data_writer, ev->obj_no)
        );
      }
      state->stream->pending_flush->len = 0;
    }

    // Record bucket pointer changes.
    if (buckets_get_dirty_bitmap_layer(state->buckets, 0)[0]) {
      ts_log(DEBUG, "Bucket pointers have changed");
      visit_bucket_dirty_bitmap(state, change_data_writer, buckets_get_dirty_bitmap_layer(state->buckets, 0)[0], 0, 0);
    }

    // Write and flush journal.
    for (uint64_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      journal_append(state->journal, c.device_offset, c.len);
    }
    // Ensure journal has been flushed to disk and written successfully.
    journal_flush(state->journal);

    // Write changes to mmap and flush.
    for (uint64_t i = 0; i < state->changes->len; i++) {
      change_t c = state->changes->elems[i];
      bucket_t* bkt = NULL;
      if (c.requires_write_lock_on_bucket_id) {
        bkt = buckets_get_bucket(state->buckets, c.requires_write_lock_on_bucket_id);
        int lock_error = pthread_rwlock_wrlock(&bkt->lock);
        if (EDEADLK != lock_error) {
          perror("Failed to acquire write lock on bucket");
          exit(EXIT_INTERNAL);
        }
      }
      memcpy(state->dev->mmap + c.device_offset, state->change_data_pool + c.offset_in_change_data_pool, c.len);
      if (bkt != NULL) {
        if (--bkt->pending_flush_changes == 0) {
          if (pthread_rwlock_unlock(&bkt->lock)) {
            perror("Failed to release write lock on bucket");
            exit(EXIT_INTERNAL);
          }
        }
      }
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
    for (uint8_t i = 0, l = 1; i < buckets_get_dirty_bitmap_layer_count(state->buckets); i++, l *= 64) {
      memset(buckets_get_dirty_bitmap_layer(state->buckets, i), 0, l * sizeof(uint64_t));
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
  state->bucket_id_pool = bucket_id_pool_create();
  state->key_pool = kh_init_str_set();
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
