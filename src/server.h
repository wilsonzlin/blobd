#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "method/_common.h"
#include "server_client.h"

typedef struct server_s server_t;

server_t* server_create(
  char* address,
  uint16_t port,
  char* unix_socket_path,
  method_ctx_t* method_ctx
);

void server_wait_epoll(server_t* server, int timeout);

void server_rearm_client_to_epoll(server_t* server, svr_client_t* client, bool read, bool write);
