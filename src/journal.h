#pragma once

#include <stdint.h>
#include "bucket.h"
#include "cursor.h"
#include "stream.h"

/**

JOURNAL
=======

Only restore from the journal if count and checksum matches.

Structure
---------

u64 xxhash_of_entries_and_count
u32 count
union ({
  u8 JOURNAL_ENTRY_TYPE_CREATE
  u48 inode_dev_offset
  u24 inode_len
  u8[] inode_data_excl_next_and_state_and_inline_data
} | {
  u8 JOURNAL_ENTRY_TYPE_COMMIT
  u48 inode_dev_offset
  u64 stream_seq_no
} | {
  u8 JOURNAL_ENTRY_TYPE_DELETE
  u48 inode_dev_offset
  u64 stream_seq_no
  u48 prev_inode_dev_offset_or_zero_if_was_head
})[] entries

**/

#define JOURNAL_ENTRY_OFFSETOF_TYPE 0

#define JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DEV_OFFSET (JOURNAL_ENTRY_OFFSETOF_TYPE + 1)
#define JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_LEN (JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DEV_OFFSET + 6)
#define JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DATA (JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_LEN + 3)
#define JOURNAL_ENTRY_CREATE_LEN(inode_len) (JOURNAL_ENTRY_CREATE_OFFSETOF_INODE_DATA + (inode_len))

#define JOURNAL_ENTRY_COMMIT_OFFSETOF_INODE_DEV_OFFSET (JOURNAL_ENTRY_OFFSETOF_TYPE + 1)
#define JOURNAL_ENTRY_COMMIT_OFFSETOF_STREAM_SEQ_NO (JOURNAL_ENTRY_COMMIT_OFFSETOF_INODE_DEV_OFFSET + 6)
#define JOURNAL_ENTRY_COMMIT_LEN (JOURNAL_ENTRY_COMMIT_OFFSETOF_STREAM_SEQ_NO + 8)

#define JOURNAL_ENTRY_DELETE_OFFSETOF_INODE_DEV_OFFSET (JOURNAL_ENTRY_OFFSETOF_TYPE + 1)
#define JOURNAL_ENTRY_DELETE_OFFSETOF_STREAM_SEQ_NO (JOURNAL_ENTRY_DELETE_OFFSETOF_INODE_DEV_OFFSET + 6)
#define JOURNAL_ENTRY_DELETE_OFFSETOF_STREAM_PREV_INODE_DEV_OFFSET (JOURNAL_ENTRY_DELETE_OFFSETOF_STREAM_SEQ_NO + 8)
#define JOURNAL_ENTRY_DELETE_LEN (JOURNAL_ENTRY_DELETE_OFFSETOF_STREAM_PREV_INODE_DEV_OFFSET + 6)

#define JOURNAL_OFFSETOF_CHECKSUM 0
#define JOURNAL_OFFSETOF_COUNT (JOURNAL_OFFSETOF_CHECKSUM + 8)
#define JOURNAL_OFFSETOF_ENTRIES (JOURNAL_OFFSETOF_COUNT + 4)

// We want to buffer as little as possible to reduce artificial response delay time/latency, but also minimise amount of SSD I/O writes. SSDs generally write in chunks up to 8 MiB (erase block), hence this value.
#define JOURNAL_RESERVED_SPACE (8 * 1024 * 1024)

typedef enum {
  JOURNAL_ENTRY_TYPE_CREATE,
  JOURNAL_ENTRY_TYPE_COMMIT,
  JOURNAL_ENTRY_TYPE_DELETE,
} journal_entry_type_t;

typedef struct {
  device_t* dev;
  uint64_t buckets_dev_offset;
  uint64_t buckets_key_mask;
  uint64_t freelist_dev_offset;
  uint64_t stream_dev_offset;
  cursor_t* mmap;
  uint32_t entry_count;
  uint64_t xxhash_u32_0;
} journal_t;

journal_t* journal_create(
  device_t* dev,
  uint64_t dev_offset,
  uint64_t buckets_dev_offset,
  uint64_t buckets_key_mask,
  uint64_t freelist_dev_offset,
  uint64_t stream_dev_offset
);

void journal_apply_online_then_clear(journal_t* jnl, buckets_t* buckets, stream_t* stream);

void journal_apply_offline_then_clear(journal_t* jnl);
