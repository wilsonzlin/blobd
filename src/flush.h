#pragma once

#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "inode.h"
#include "journal.h"
#include "server.h"
#include "server_client.h"
#include "stream.h"

typedef struct flush_state_s flush_state_t;

void flush_lock_tasks(flush_state_t* state);

void flush_unlock_tasks(flush_state_t* state);

typedef struct {
  void* future;
  uint32_t pos;
  uint8_t count;
} flush_task_reserve_t;

flush_task_reserve_t flush_reserve_task(flush_state_t* state, uint8_t count, uint32_t len, svr_client_t* client_or_null, uint64_t delete_inode_dev_offset_or_zero);

cursor_t* flush_get_reserved_cursor(flush_task_reserve_t r);

void flush_commit_task(flush_state_t* state, flush_task_reserve_t r);

void flush_add_write_task(flush_state_t* state, svr_client_t* client, uint64_t write_bytes);

flush_state_t* flush_state_create(
  buckets_t* buckets,
  device_t* dev,
  freelist_t* freelist,
  journal_t* journal,
  server_t* server,
  stream_t* stream
);

void* flush_worker_start(flush_state_t* state);

void flush_worker_join(void* handle);
