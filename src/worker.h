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
} worker_method_handler_ctx_t;

typedef struct worker_s worker_t;

worker_t* worker_create(
  device_t* dev,
  buckets_t* bkts
);

// Returns a handle that is passed to workers_join.
void* workers_start(
  worker_t* worker,
  uint64_t count
);

void workers_join(
  void* handle,
  uint64_t count
);
