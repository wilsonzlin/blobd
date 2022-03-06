#pragma once

#include "flush.h"
#include "server.h"

typedef struct {
  buckets_t* bkts;
  device_t* dev;
  flush_state_t* flush_state;
  freelist_t* fl;
  inodes_state_t* inodes_state;
  stream_t* stream;
} manager_method_handler_ctx_t;

typedef struct manager_s manager_t;

manager_t* manager_create(
  buckets_t* bkts,
  device_t* dev,
  flush_state_t* flush_state,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  stream_t* stream
);

// Returns a handle that is passed to manager_join.
void* manager_start(manager_t* mgr);

void manager_join(void* handle);
