#pragma once

#include <stdatomic.h>
#include <stdint.h>
#include "method/_common.h"
#include "server_method_args.h"

typedef enum {
  SVR_CLIENT_RESULT__UNKNOWN,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE,
  SVR_CLIENT_RESULT_AWAITING_FLUSH,
  SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR,
  SVR_CLIENT_RESULT_END,
} svr_client_result_t;

typedef enum {
  SVR_CLIENT_STATE_INIT,
  SVR_CLIENT_STATE_ARGS_READ,
} svr_client_state_t;

typedef struct svr_client_s {
  // This doesn't need to be atomic; only one thread can ever see/touch/manage a svr_client_t at one time.
  int fd;
  svr_method_args_parser_t args_parser;
  method_t method;
  method_state_t method_state;
  // For server_client.c internal use only.
  _Atomic(struct svr_client_s*) next_free_in_pool;
} svr_client_t;

void server_client_reset(svr_client_t* client);

typedef struct server_clients_s server_clients_t;

server_clients_t* server_clients_create();

// Initialises a new svr_client_t and returns it.
svr_client_t* server_clients_add(server_clients_t* clients, int client_fd);

// Closes the associated FD, and releases the svr_client_t back to the pool.
void server_clients_close(server_clients_t* clients, svr_client_t* client);
