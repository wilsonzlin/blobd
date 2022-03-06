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
#include "stream.h"
#include "tile.h"
#include "util.h"
#include "vec.h"

LOGGER("flush");

typedef struct {
  uint64_t device_offset;
  uint32_t len;
  uint32_t offset_in_change_data_pool;
} change_t;

LIST_DEF(changes, change_t);
LIST(changes, change_t);

LIST_DEF(inode_list, inode_t*);
LIST(inode_list, inode_t*);

struct flush_state_s {
  device_t* dev;
  journal_t* journal;
  freelist_t* fl;
  inodes_state_t* inodes_state;
  buckets_t* buckets;
  stream_t* stream;
  inode_list_t* inodes_awaiting_refcount_for_deletion;
  changes_t* changes;
  uint8_t* change_data_pool;
  uint64_t change_data_pool_cap;
};

void flush_mark_inode_for_awaiting_deletion(
  flush_state_t* state,
  uint64_t bkt_id,
  inode_t* previous_if_inode_or_null_if_bucket,
  inode_t* ino
) {
  // We don't need to write to mmap, as we'll do it later during flush. We could also do it now if we wanted to.
  atomic_store_explicit(&ino->state, INO_STATE_DELETED, memory_order_relaxed);
  // We can safely read and write in multiple operations as we're running in a single-threaded manager.
  inode_t* next = atomic_load_explicit(&ino->next, memory_order_relaxed);
  if (previous_if_inode_or_null_if_bucket == NULL) {
    bucket_t* bkt = buckets_get_bucket(state->buckets, bkt_id);
    atomic_store_explicit(&bkt->head, next, memory_order_relaxed);
  } else {
    atomic_store_explicit(&previous_if_inode_or_null_if_bucket->next, next, memory_order_relaxed);
  }
  inode_list_append(state->inodes_awaiting_refcount_for_deletion, ino);
  stream_event_t ev = {
    .bkt_id = bkt_id,
    .obj_no = read_u64(state->dev->mmap + (TILE_SIZE * ino->tile) + ino->tile_offset + INO_OFFSETOF_OBJ_NO),
    .seq_no = state->stream->next_seq_no++,
    .typ = STREAM_EVENT_OBJECT_DELETE,
  };
  events_pending_flush_append(state->stream->pending_flush, ev);
  buckets_mark_bucket_as_dirty(state->buckets, bkt_id);
}

void flush_mark_inode_as_committed(
  flush_state_t* state,
  uint64_t bkt_id,
  inode_t* ino
) {
  // We don't need to write to mmap, as we'll do it later during flush. We could also do it now if we wanted to.
  // TODO Do we care that someone could still be writing to the object?
  atomic_store_explicit(&ino->state, INO_STATE_READY, memory_order_relaxed);
  stream_event_t ev = {
    .bkt_id = bkt_id,
    .obj_no = read_u64(state->dev->mmap + (TILE_SIZE * ino->tile) + ino->tile_offset + INO_OFFSETOF_OBJ_NO),
    .seq_no = state->stream->next_seq_no++,
    .typ = STREAM_EVENT_OBJECT_COMMIT,
  };
  events_pending_flush_append(state->stream->pending_flush, ev);
  buckets_mark_bucket_as_dirty(state->buckets, bkt_id);
}

typedef struct {
  flush_state_t* state;
  uint64_t change_data_next;
} change_data_pool_writer_t;

// Returns 1 if a new change was appended or 0 if the previous change was simply extended.
// TODO Assert total bytes does not exceed journal space.
static inline uint8_t append_change(
  changes_t* changes,
  uint64_t change_data_offset,
  uint64_t device_offset,
  uint32_t len
) {
  change_t* last = changes_last_mut(changes);
  if (last != NULL && last->device_offset + last->len == device_offset && last->offset_in_change_data_pool + last->len == change_data_offset) {
    ASSERT_STATE(last->len <= 4294967295 - len, "too many bytes changed in one entry");
    last->len += len;
    return 0;
  } else {
    change_t c = {
      .device_offset = device_offset,
      .len = len,
      .offset_in_change_data_pool = change_data_offset,
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
  memcpy(writer->state->change_data_pool + writer->change_data_next, data, len);
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

// WARNING: Function call argument evaluation order is undefined in C, so we cannot rely on it to simplify this expression. We must get `change_data_next` before evaluating `c1`/`c2`/`c3`.
// We avoid `(c1) + (c2) + (c3)` as the evaluation order is unspecified.
// NOTE: This is only supported by GCC (https://gcc.gnu.org/onlinedocs/gcc/Statement-Exprs.html).
#define __RECORD_CHANGE(dev_offset, c1, c2, c3) \
  ({ uint64_t __cdn = change_data_writer->change_data_next; uint32_t __len = c1; __len += c2; __len += c3; append_change(state->changes, __cdn, dev_offset, __len); })

#define RECORD_CHANGE1(dev_offset, c1) __RECORD_CHANGE(dev_offset, c1, 0, 0)

#define RECORD_CHANGE2(dev_offset, c1, c2) __RECORD_CHANGE(dev_offset, c1, c2, 0)

#define RECORD_CHANGE3(dev_offset, c1, c2, c3) __RECORD_CHANGE(dev_offset, c1, c2, c3)

static inline void ts_log_debug_writing_change(change_t c, buckets_t* buckets) {
  uint64_t offset = 0;
  if (c.device_offset < offset + JOURNAL_RESERVED_SPACE) {
    DEBUG_TS_LOG("Writing journal area change: %lu, %u bytes", c.device_offset - offset, c.len);
    return;
  }
  offset += JOURNAL_RESERVED_SPACE;
  if (c.device_offset < offset + STREAM_RESERVED_SPACE) {
    DEBUG_TS_LOG("Writing stream area change: %lu, %u bytes", c.device_offset - offset, c.len);
    return;
  }
  offset += STREAM_RESERVED_SPACE;
  if (c.device_offset < offset + FREELIST_RESERVED_SPACE) {
    DEBUG_TS_LOG("Writing freelist area change: %lu, %u bytes", c.device_offset - offset, c.len);
    return;
  }
  offset += FREELIST_RESERVED_SPACE;
  uint64_t buckets_space = BUCKETS_RESERVED_SPACE(buckets_get_count(buckets));
  if (c.device_offset < offset + buckets_space) {
    DEBUG_TS_LOG("Writing buckets area change: %lu, %u bytes", c.device_offset - offset, c.len);
    return;
  }
  offset += buckets_space;
  DEBUG_TS_LOG("Writing heap area change: %lu, %u bytes", c.device_offset - offset, c.len);
}

// This will update bucket head, as well as "next" and "state" fields on all inodes in the bucket.
static inline void record_bucket_change(
  flush_state_t* state,
  change_data_pool_writer_t* change_data_writer,
  uint64_t bkt_id
) {
  bucket_t* bkt = buckets_get_bucket(state->buckets, bkt_id);
  uint64_t dev_offset = buckets_get_device_offset_of_bucket(state->buckets, bkt_id);
  inode_t* ino = atomic_load_explicit(&bkt->head, memory_order_relaxed);
  while (true) {
    RECORD_CHANGE2(
      dev_offset,
      change_data_pool_append_u24(change_data_writer, ino == NULL ? 0 : ino->tile),
      change_data_pool_append_u24(change_data_writer, ino == NULL ? 0 : ino->tile_offset)
    );
    if (ino == NULL) {
      break;
    }
    RECORD_CHANGE1(
      (TILE_SIZE * ino->tile) + ino->tile_offset + INO_OFFSETOF_STATE,
      atomic_load_explicit(&ino->state, memory_order_relaxed)
    );
    dev_offset = (TILE_SIZE * ino->tile) + ino->tile_offset + INO_OFFSETOF_NEXT_INODE_TILE;
    ino = atomic_load_explicit(&ino->next, memory_order_relaxed);
  }
}

void visit_bucket_dirty_bitmap(
  flush_state_t* state,
  change_data_pool_writer_t* change_data_writer,
  uint64_t bitmap,
  uint64_t base,
  uint8_t layer
) {
  buckets_t* bkts = state->buckets;
  array_u8_64_t candidates = vec_find_indices_of_nonzero_bits_64(bitmap);
  if (layer == buckets_get_dirty_bitmap_layer_count(bkts) - 1) {
    VEC_ITER_INDICES_OF_NONZERO_BITS_64(candidates, index) {
      uint64_t bkt_id = base + index;
      record_bucket_change(state, change_data_writer, bkt_id);
    }
  } else {
    VEC_ITER_INDICES_OF_NONZERO_BITS_64(candidates, index) {
      uint64_t offset = base + index;
      visit_bucket_dirty_bitmap(state, change_data_writer, buckets_get_dirty_bitmap_layer(bkts, layer + 1)[offset], offset * 64, layer + 1);
    }
  }
}

void flush_perform(flush_state_t* state) {
  DEBUG_TS_LOG("Starting flush");

  // We collect changes to make first, and then write to journal. This allows two optimisations:
  // - Avoiding paging in the journal until we need to.
  // - Compacting contiguous change list entries, before committing final list to journal.

  change_data_pool_writer_t cdw = {
    .change_data_next = 0,
    .state = state,
  };
  change_data_pool_writer_t* change_data_writer = &cdw;

  // Process inodes pending deletion.
  uint64_t new_awaiting_deletion_len = 0;
  for (uint64_t i = 0; i < state->inodes_awaiting_refcount_for_deletion->len; i++) {
    inode_t* ino = state->inodes_awaiting_refcount_for_deletion->elems[i];
    if (atomic_load_explicit(&ino->refcount, memory_order_relaxed) == 0) {
      freelist_replenish_tiles_of_inode(state->fl, state->dev->mmap + (TILE_SIZE * ino->tile) + ino->tile_offset);
      inode_destroy_thread_unsafe(state->inodes_state, ino);
    } else {
      state->inodes_awaiting_refcount_for_deletion->elems[new_awaiting_deletion_len] = ino;
      new_awaiting_deletion_len++;
    }
  }
  state->inodes_awaiting_refcount_for_deletion->len = new_awaiting_deletion_len;

  // Record freelist changes.
  // NOTE: Do this after processing object deletes, as those can cause space to be freed.
  if (state->fl->dirty_tiles_bitmap_1) {
    array_u8_64_t i1_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_1);
    VEC_ITER_INDICES_OF_NONZERO_BITS_64(i1_candidates, i1) {
      array_u8_64_t i2_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_2[i1]);
      VEC_ITER_INDICES_OF_NONZERO_BITS_64(i2_candidates, i2) {
        array_u8_64_t i3_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_3[i1 * 64 + i2]);
        VEC_ITER_INDICES_OF_NONZERO_BITS_64(i3_candidates, i3) {
          array_u8_64_t i4_candidates = vec_find_indices_of_nonzero_bits_64(state->fl->dirty_tiles_bitmap_4[(((i1 * 64) + i2) * 64) + i3]);
          VEC_ITER_INDICES_OF_NONZERO_BITS_64(i4_candidates, i4) {
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
  if (state->stream->pending_flush->len) {
    uint64_t new_obj_no = atomic_load_explicit(&state->stream->next_obj_no, memory_order_relaxed);
    uint64_t new_seq_no = atomic_load_explicit(&state->stream->next_seq_no, memory_order_relaxed);
    DEBUG_TS_LOG("New object number is %lu, sequence number %lu", new_obj_no, new_seq_no);
    RECORD_CHANGE2(
      state->stream->dev_offset,
      change_data_pool_append_u64(change_data_writer, new_obj_no),
      change_data_pool_append_u64(change_data_writer, new_seq_no)
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
    #ifdef TURBOSTORE_DEBUG_LOG_FLUSH_DELTAS
      ts_log_debug_writing_change(c, state->buckets);
    #endif
    memcpy(state->dev->mmap + c.device_offset, state->change_data_pool + c.offset_in_change_data_pool, c.len);
  }
  // We must ensure changes have been written successfully BEFORE erasing journal.
  // TODO Is granular but more frequent msync() calls better or worse?
  device_sync(state->dev, 0, state->dev->size);
  state->changes->len = 0;

  // Erase and flush journal.
  journal_clear(state->journal);

  // Clear dirty bitmaps.
  state->fl->dirty_tiles_bitmap_1 = 0;
  memset(state->fl->dirty_tiles_bitmap_2, 0, 64 * sizeof(uint64_t));
  memset(state->fl->dirty_tiles_bitmap_3, 0, 64 * 64 * sizeof(uint64_t));
  memset(state->fl->dirty_tiles_bitmap_4, 0, 64 * 64 * 64 * sizeof(uint64_t));
  // Use uint64_t for `l`.
  for (uint64_t i = 0, l = 1; i < buckets_get_dirty_bitmap_layer_count(state->buckets); i++, l *= 64) {
    memset(buckets_get_dirty_bitmap_layer(state->buckets, i), 0, l * sizeof(uint64_t));
  }

  DEBUG_TS_LOG("Flush ended");
}

flush_state_t* flush_state_create(
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  buckets_t* buckets,
  stream_t* stream
) {
  flush_state_t* state = malloc(sizeof(flush_state_t));
  state->dev = dev;
  state->journal = journal;
  state->fl = fl;
  state->inodes_state = inodes_state;
  state->buckets = buckets;
  state->stream = stream;
  state->inodes_awaiting_refcount_for_deletion = inode_list_create();
  state->changes = changes_create_with_capacity(128);
  state->change_data_pool_cap = 2048;
  state->change_data_pool = malloc(state->change_data_pool_cap);
  return state;
}
