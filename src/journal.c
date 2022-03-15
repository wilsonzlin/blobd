#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "../ext/xxHash/xxhash.h"
#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "journal.h"
#include "log.h"
#include "stream.h"
#include "util.h"

LOGGER("journal");

static inline void sync_journal(journal_t* jnl) {
  device_sync(jnl->dev, 0, JOURNAL_RESERVED_SPACE);
}

journal_t* journal_create(
  device_t* dev,
  uint64_t dev_offset,
  uint64_t buckets_dev_offset,
  uint64_t buckets_key_mask,
  uint64_t freelist_dev_offset,
  uint64_t stream_dev_offset
) {
  journal_t* journal = malloc(sizeof(journal_t));
  journal->dev = dev;
  journal->buckets_dev_offset = buckets_dev_offset;
  journal->buckets_key_mask = buckets_key_mask;
  journal->freelist_dev_offset = freelist_dev_offset;
  journal->stream_dev_offset = stream_dev_offset;
  journal->mmap = dev->mmap + dev_offset;
  journal->entry_count = 0;
  uint8_t u32_0[4];
  write_u32(u32_0, 0);
  journal->xxhash_u32_0 = XXH3_64bits(u32_0, 4);
  return journal;
}

static void journal_clear(journal_t* jnl) {
  jnl->entry_count = 0;
  write_u64(jnl->mmap, jnl->xxhash_u32_0);
  write_u32(jnl->mmap + 4, 0);
  // We must flush, or else we'll try recovering from journal and overwrite data.
  sync_journal(jnl);
}

#define RECORD_STREAM_EVENT(event_type) { \
    if (stream != NULL) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&stream->rwlock), "lock stream"); \
    cursor_t* stream_event_cur = jnl->dev->mmap + jnl->stream_dev_offset + STREAM_OFFSETOF_EVENT(stream_seq_no % STREAM_EVENTS_BUF_LEN); \
    produce_u8(&stream_event_cur, event_type); \
    produce_u40(&stream_event_cur, bkt_id); \
    produce_u64(&stream_event_cur, stream_seq_no); \
    produce_u64(&stream_event_cur, obj_no); \
    if (stream != NULL) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&stream->rwlock), "unlock stream"); \
  }

static void journal_apply_and_clear(
  journal_t* jnl,
  // The following must be NULL if offline and not NULL if online.
  buckets_t* buckets,
  stream_t* stream
) {
  cursor_t* start = jnl->mmap + 8;
  cursor_t* cur = start;
  uint32_t count = consume_u32(&cur);
  uint64_t max_obj_no = 0;
  uint64_t max_seq_no = 0;
  for (uint32_t i = 0; i < count; i++) {
    journal_entry_type_t typ = *cur++;
    uint64_t inode_dev_offset = consume_u48(&cur);
    cursor_t* inode_cur = jnl->dev->mmap + inode_dev_offset;

    if (JOURNAL_ENTRY_TYPE_CREATE == typ) {
      uint64_t inode_len = consume_u24(&cur);
      cursor_t* read_cur = cur;
      uint64_t obj_size = read_u40(read_cur + INO_OFFSETOF_SIZE);
      uint64_t obj_no = read_u64(read_cur + INO_OFFSETOF_OBJ_NO);
      // TODO Should we also check against existing next_obj_no on device?
      max_obj_no = max(max_obj_no, obj_no);
      uint8_t key_len = read_cur[INO_OFFSETOF_KEY_LEN];
      ino_last_tile_mode_t ltm = read_cur[INO_OFFSETOF_LAST_TILE_MODE];
      uint64_t tile_count;
      uint64_t inline_data_len;
      if (ltm == INO_LAST_TILE_MODE_INLINE) {
        tile_count = obj_size / TILE_SIZE;
        inline_data_len = obj_size % TILE_SIZE;
      } else {
        tile_count = uint_divide_ceil(obj_size, TILE_SIZE);
        inline_data_len = 0;
      }
      uint32_t ino_size = INO_SIZE(key_len, tile_count, inline_data_len);

      // Update microtile free space.
      // WARNING: Journal events must be recorded and applied in order of creates as otherwise microtile value will be corrupted. If B is created after A but gets recorded in an earlier journal, then when A is applied the microtile free space value will become corrupted.
      write_u24(
        jnl->dev->mmap + FREELIST_OFFSETOF_TILE(inode_dev_offset / TILE_SIZE),
        TILE_SIZE - inode_dev_offset - ino_size
      );

      // Mark tiles as used.
      for (uint64_t i = 0; i < tile_count; i++) {
        uint32_t tile_no = read_u24(read_cur + INO_OFFSETOF_TILE_NO(key_len, i));
        write_u24(jnl->dev->mmap + FREELIST_OFFSETOF_TILE(tile_no), 16777214);
      }

      // Write inode to heap.
      memcpy(jnl->dev->mmap + inode_dev_offset, cur, inode_len);
    } else {
      uint64_t stream_seq_no = consume_u64(&cur);
      max_seq_no = max(max_seq_no, stream_seq_no);
      uint64_t old_data = consume_u48(&cur);
      uint8_t key_len = inode_cur[INO_OFFSETOF_KEY_LEN];
      uint64_t bkt_id = BUCKET_ID_FOR_KEY(inode_cur + INO_OFFSETOF_KEY, key_len, jnl->buckets_key_mask);
      uint64_t obj_no = read_u64(inode_cur + INO_OFFSETOF_OBJ_NO);

      if (JOURNAL_ENTRY_TYPE_COMMIT == typ) {
        // Update state. No locks necessary; reading/writing one byte is always atomic.
        inode_cur[INO_OFFSETOF_STATE] = INO_STATE_READY;

        // Update bucket head.
        write_u48(
          jnl->dev->mmap + jnl->buckets_dev_offset + BUCKETS_OFFSETOF_BUCKET(bkt_id),
          inode_dev_offset
        );

        // Record stream event.
        RECORD_STREAM_EVENT(STREAM_EVENT_OBJECT_COMMIT);
      } else if (JOURNAL_ENTRY_TYPE_DELETE == typ) {
        uint64_t tile_count = INODE_TILE_COUNT(inode_cur);

        // Update state. No locks necessary; reading/writing one byte is always atomic.
        inode_cur[INO_OFFSETOF_STATE] = INO_STATE_DELETED;

        // Update previous inode or bucket head.
        uint64_t next_inode_dev_offset = read_u48(inode_cur + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET);
        if (buckets != NULL) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&buckets->bucket_locks[bkt_id]), "lock bucket");
        if (old_data) {
          // `old_data` represents inode dev offset of previous inode.
          cursor_t* prev_inode_cur = jnl->dev->mmap + old_data;
          write_u48(
            prev_inode_cur + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET,
            next_inode_dev_offset
          );
        } else {
          // Deleted inode was head, so update bucket head.
          write_u48(
            jnl->dev->mmap + jnl->buckets_dev_offset + BUCKETS_OFFSETOF_BUCKET(bkt_id),
            next_inode_dev_offset
          );
        }
        if (buckets != NULL) ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&buckets->bucket_locks[bkt_id]), "lock bucket");

        // Release tiles back to freelist on device.
        // NOTE: In-memory freelist_t values/data structures not affected/updated/synced/reloaded.
        for (uint64_t i = 0; i < tile_count; i++) {
          uint32_t tile_no = read_u24(inode_cur + INO_OFFSETOF_TILE_NO(key_len, i));
          write_u24(jnl->dev->mmap + FREELIST_OFFSETOF_TILE(tile_no), 16777215);
        }

        // Record stream event.
        RECORD_STREAM_EVENT(STREAM_EVENT_OBJECT_DELETE);
      } else {
        ASSERT_UNREACHABLE("journal entry type: %d", typ);
      }
    }
  }

  if (max_seq_no) {
    write_u64(jnl->dev->mmap + jnl->stream_dev_offset + STREAM_OFFSETOF_NEXT_SEQ_NO, max_seq_no + 1);
  }
  if (max_obj_no) {
    write_u64(jnl->dev->mmap + jnl->stream_dev_offset + STREAM_OFFSETOF_NEXT_OBJ_NO, max_obj_no + 1);
  }

  // Ensure sync BEFORE clearing journal.
  device_sync(jnl->dev, 0, jnl->dev->size);
  journal_clear(jnl);
}

void journal_apply_online_then_clear(journal_t* jnl, buckets_t* buckets, stream_t* stream) {
  journal_apply_and_clear(jnl, buckets, stream);
}

void journal_apply_offline_then_clear(journal_t* jnl) {
  uint64_t checksum_recorded = read_u64(jnl->mmap);
  cursor_t* start = jnl->mmap + 8;
  // TODO Ensure not greater than JOURNAL_ENTRIES_CAP.
  uint32_t count = read_u32(start);
  uint64_t checksum_actual = XXH3_64bits(start, 8 + count * (1 + 8 + 3));
  if (checksum_recorded != checksum_actual) {
    // Just because the checksum doesn't match doesn't mean there's corruption:
    // - The process might've crashed while writing the journal.
    // - The process might've crashed while clearing the journal.
    ts_log(WARN, "Journal is not intact and will not be applied");
    // For extra safety, don't clear journal in case we want to look at it.
  } else if (count) {
    ts_log(WARN, "Intact journal found, applying");
    journal_apply_and_clear(jnl, NULL, NULL);
  }
}
