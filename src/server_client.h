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

typedef void (method_state_destructor_t)(void*);

typedef struct svr_client_s svr_client_t;

int server_client_get_fd(svr_client_t* client);

svr_method_args_parser_t* server_client_get_args_parser(svr_client_t* client);

void server_client_set_args_parser(svr_client_t* client, svr_method_args_parser_t* args_parser);

method_t server_client_get_method(svr_client_t* client);

void server_client_set_method(svr_client_t* client, method_t method);

void* server_client_get_method_state(svr_client_t* client);

void server_client_set_method_state(svr_client_t* client, void* method_state);

void server_client_set_method_state_destructor(svr_client_t* client, method_state_destructor_t* destructor);

// Allows the client to start handling a new request without having to destroy it and create a new one.
void server_client_reset(svr_client_t* client);

typedef struct server_clients_s server_clients_t;

server_clients_t* server_clients_create();

// Initialises a new svr_client_t and returns it.
svr_client_t* server_clients_add(server_clients_t* clients, int client_fd);

// Closes the associated FD, destroys any client state (e.g. args_parser, method_state), and frees the svr_client_t.
void server_clients_close(server_clients_t* clients, svr_client_t* client);
