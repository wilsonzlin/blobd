#pragma once

#include <stdbool.h>
#include "server_client.h"
#include "server_method_args.h"

typedef struct server_s server_t;

// Called with callback_state and svr_client_t*.
typedef void (server_on_client_event_handler)(void*, svr_client_t*);

// Called with method_ctx and args_parser; returns method_state.
typedef void* (server_method_state_creator)(void*, svr_method_args_parser_t*);

// Called with method_ctx, method_state, and client_fd; returns client_result.
typedef svr_client_result_t (server_method_handler)(void*, void*, int);

// Called with method_state.
typedef void (server_method_state_destructor)(void*);

typedef struct server_methods_s server_methods_t;

server_methods_t* server_methods_create();

void server_methods_add(server_methods_t* methods, method_t method, server_method_state_creator* state_creator, server_method_handler* handler, server_method_state_destructor* destructor);

server_t* server_create(
  char* unix_socket_path,
  void* callback_state,
  server_on_client_event_handler* on_client_event,
  void* method_ctx,
  server_methods_t* methods
);

void server_wait_epoll(server_t* server, int timeout);

void server_rearm_client_to_epoll(server_t* server, svr_client_t* client, bool read, bool write);

svr_client_result_t server_process_client_until_result(server_t* server, svr_client_t* client);

void server_close_client(server_t* server, svr_client_t* client);
