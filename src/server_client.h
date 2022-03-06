#pragma once

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

typedef struct {
  int fd;
  svr_method_args_parser_t* args_parser;
  method_t method;
  void* method_state;
  void (*method_state_destructor)(void*);
} svr_client_t;

typedef struct server_clients_s server_clients_t;

server_clients_t* server_clients_create();

// Initialises a new svr_client_t and returns it.
svr_client_t* server_clients_add(server_clients_t* clients, int client_fd);

svr_client_t* server_clients_get(server_clients_t* clients, int client_fd);

// Closes the associated FD, destroys any client state (e.g. args_parser, method_state), and frees the svr_client_t.
void server_clients_close(server_clients_t* clients, svr_client_t* client);
