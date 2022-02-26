#pragma once

#include <pthread.h>
#include "bucket.h"
#include "freelist.h"
#include "../ext/klib/khash.h"

/**

SERVER
======

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

typedef struct svr_method_args_parser_t;

// Returns NULL if not enough bytes.
uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, size_t want_bytes);

void svr_method_args_parser_end(svr_method_args_parser_t* parser);

typedef struct {
  device_t* dev;
  flush_state_t* flush;
  freelist_t* fl;
  buckets_t* bkts;
} svr_method_handler_ctx_t;

typedef struct svr_client_t;

LIST_DEF(client_ids, uint32_t);

KHASH_DECLARE(svr_fd_to_client, int, uint32_t);

typedef struct {
  kh_svr_fd_to_client_t* fd_to_client;
  pthread_rwlock_t fd_to_client_lock;
  svr_client_t* ring_buf;
  size_t ring_buf_cap_log2;
  size_t ring_buf_read;
  size_t ring_buf_write;
  // The size of this ring buffer should also be ring_buf_cap_log2.
  uint32_t* ready_ring_buf;
  size_t ready_ring_buf_read;
  size_t ready_ring_buf_write;
  pthread_mutex_t ready_lock;
  client_ids_t* awaiting_flush;
  pthread_mutex_t awaiting_flush_lock;
} svr_clients_t;

// This never returns.
void server_start_loop(
  svr_clients_t* svr,
  size_t worker_count,
  device_t* dev,
  flush_state_t* flush,
  freelist_t* fl,
  buckets_t* bkts
);
