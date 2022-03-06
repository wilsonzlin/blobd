#pragma once

#include <stdbool.h>
#include <pthread.h>
#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "list.h"
#include "manager.h"
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
  SVR_CLIENT_RESULT_HANDED_OFF_TO_MANAGER,
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
  freelist_t* fl;
  inodes_state_t* inodes_state;
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

typedef struct svr_client_s svr_client_t;

svr_method_t server_client_get_method(svr_client_t* client);

void* server_client_get_method_state(svr_client_t* client);

int server_client_get_file_descriptor(svr_client_t* client);

typedef struct server_s server_t;

server_t* server_create(
  device_t* dev,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  buckets_t* bkts,
  stream_t* stream,
  manager_state_t* manager_state
);

svr_method_handler_ctx_t* server_get_method_handler_context(server_t* server);

void server_hand_back_client_from_manager(server_t* clients, svr_client_t* client, svr_client_result_t result);

// This never returns.
void server_start_loop(
  server_t* server,
  uint64_t worker_count
);
