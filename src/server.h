#pragma once

#include <stdbool.h>
#include <pthread.h>
#include "bucket.h"
#include "device.h"
#include "flushstate.h"
#include "freelist.h"
#include "list.h"
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

typedef enum {
  SVR_CLIENT_RESULT__UNKNOWN,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE,
  SVR_CLIENT_RESULT_AWAITING_FLUSH,
  SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR,
  SVR_CLIENT_RESULT_END,
} svr_client_result_t;

typedef struct svr_method_args_parser_s svr_method_args_parser_t;

// Returns NULL if not enough bytes.
uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, uint64_t want_bytes);

// Returns false if not all argument bytes were used.
bool svr_method_args_parser_end(svr_method_args_parser_t* parser);

typedef struct {
  device_t* dev;
  flush_state_t* flush;
  freelist_t* fl;
  buckets_t* bkts;
  stream_t* stream;
} svr_method_handler_ctx_t;

typedef enum {
  // Dummy value.
  SVR_METHOD__UNKNOWN = 0,
  SVR_METHOD_CREATE_OBJECT = 1,
  SVR_METHOD_INSPECT_OBJECT = 2,
  SVR_METHOD_READ_OBJECT = 3,
  SVR_METHOD_WRITE_OBJECT = 4,
  SVR_METHOD_COMMIT_OBJECT = 5,
  SVR_METHOD_DELETE_OBJECT = 6,
} svr_method_t;

typedef struct server_s server_t;

server_t* server_create(
  device_t* dev,
  flush_state_t* flush,
  freelist_t* fl,
  buckets_t* bkts
);

// Returns false if no clients are awaiting flush.
bool server_on_flush_start(server_t* server);

void server_on_flush_end(server_t* server);

// This never returns.
void server_start_loop(
  server_t* server,
  uint64_t worker_count
);
