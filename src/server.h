#pragma once

#include <pthread.h>
#include "bucket.h"
#include "device.h"
#include "flushstate.h"
#include "freelist.h"
#include "list.h"
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
  SVR_CLIENT_STATE_AWAITING_CLIENT_IO,
  SVR_CLIENT_STATE_AWAITING_FLUSH,
  SVR_CLIENT_STATE_READY,
} svr_client_state_t;

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
uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, size_t want_bytes);

// Returns false if not all argument bytes were used.
bool svr_method_args_parser_end(svr_method_args_parser_t* parser);

typedef struct {
  device_t* dev;
  flush_state_t* flush;
  freelist_t* fl;
  buckets_t* bkts;
} svr_method_handler_ctx_t;

typedef enum {
  // Dummy value.
  SVR_METHOD__UNKNOWN = 0,
  SVR_METHOD_CREATE_OBJECT = 1,
  // (u8 key_len, char[] key, i64 start, i64 end_exclusive_or_zero_for_eof).
  SVR_METHOD_READ_OBJECT = 2,
  // (u8 key_len, char[] key, i64 start, i64 end_exclusive).
  SVR_METHOD_WRITE_OBJECT = 3,
  // (u8 key_len, char[] key).
  SVR_METHOD_DELETE_OBJECT = 4,
} svr_method_t;

typedef struct {
  atomic_uint_least8_t open;
  atomic_uint_least8_t state;
  int fd;
  svr_method_args_parser_t* args_parser;
  svr_method_t method;
  void* method_state;
  void (*method_state_destructor)(void*);
} svr_client_t;

typedef struct svr_clients_s svr_clients_t;

svr_clients_t* server_clients_create(size_t max_clients_log2);

size_t server_clients_get_capacity(svr_clients_t* clients);

void server_clients_acquire_awaiting_flush_lock(svr_clients_t* clients);

void server_clients_release_awaiting_flush_lock(svr_clients_t* clients);

size_t server_clients_get_awaiting_flush_count(svr_clients_t* clients);

void server_clients_pop_all_awaiting_flush(svr_clients_t* clients, uint32_t* out);

void server_clients_push_all_ready(svr_clients_t* clients, uint32_t* client_ids, size_t n);

// This never returns.
void server_start_loop(
  svr_clients_t* svr,
  size_t worker_count,
  device_t* dev,
  flush_state_t* flush,
  freelist_t* fl,
  buckets_t* bkts
);
