#pragma once

#include <stdbool.h>
#include <pthread.h>
#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "flush.h"
#include "list.h"
#include "manager_state.h"
#include "stream.h"
#include "../ext/klib/khash.h"

/**

SERVER
======

The server is like RPC. There are methods, with one signature each. The arguments are serialised into a binary format and stored sequentially, like an ABI calling convention. Then the handler will read and write directly between the memory and socket (no user space buffering). The handler must do as much work as possible, then yield back ASAP for cooperative multitasking on one thread. This could be when writing to socket or flushing to disk. Even if we use multiple threads, it's still not ideal to waste threads by blocking on I/O tasks and idling CPU.
The client must pass the length of the serialised arguments list in bytes. The server will only parse them once all bytes have been received.
The server will use epoll to read and write to sockets, and then call handlers to resume handling after bytes are available/have been written.
There is no multiplexing. A client must only send new calls after old calls have been responded. If there is an error, the client is simply disconnected.

We must use ONESHOT to avoid race conditions: [worker] read() EAGAIN => [main] epoll_wait emits event on client => [main] client state is not READY => [worker] client state is changed to READY.

**/

typedef struct {
  buckets_t* bkts;
  device_t* dev;
  flush_state_t* flush_state;
  freelist_t* fl;
  inodes_state_t* inodes_state;
  stream_t* stream;
} svr_method_handler_ctx_t;

typedef struct server_s server_t;

server_t* server_create(
  device_t* dev,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  buckets_t* bkts,
  stream_t* stream,
  flush_state_t* flush_state,
  manager_state_t* manager_state
);

svr_method_handler_ctx_t* server_get_method_handler_context(server_t* server);

void server_hand_back_client_from_manager(server_t* clients, svr_client_t* client, svr_client_result_t result);

// This never returns.
void server_start_loop(
  server_t* server,
  uint64_t worker_count
);
