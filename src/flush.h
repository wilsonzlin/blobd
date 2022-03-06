#pragma once

#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "inode.h"
#include "journal.h"
#include "stream.h"

typedef struct flush_state_s flush_state_t;

void flush_mark_inode_for_awaiting_deletion(
  flush_state_t* state,
  uint64_t bkt_id,
  inode_t* previous_if_inode_or_null_if_head,
  inode_t* ino
);

void flush_mark_inode_as_committed(
  flush_state_t* state,
  uint64_t bkt_id,
  inode_t* ino
);

void flush_perform(flush_state_t* state);

flush_state_t* flush_state_create(
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  buckets_t* buckets,
  stream_t* stream
);
