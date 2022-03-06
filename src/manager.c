#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "exit.h"
#include "flush.h"
#include "list.h"
#include "log.h"
#include "manager.h"
#include "method/_common.h"
#include "method/commit_object.h"
#include "method/create_object.h"
#include "method/delete_object.h"
#include "server_client.h"
#include "server.h"

#define MANAGER_SOCK_PATH "/tmp/turbostore-manager.sock"

LOGGER("manager");

LIST_DEF(clients_awaiting_flush, svr_client_t*);
LIST(clients_awaiting_flush, svr_client_t*);

KHASH_MAP_INIT_INT(fd_to_client, svr_client_t*);

struct manager_s {
  server_clients_t* clients;
  server_t* server;

  manager_method_handler_ctx_t method_handler_ctx;

  flush_state_t* flush_state;
  struct timespec last_flushed;
  clients_awaiting_flush_t* clients_awaiting_flush;
};

void manager_on_client_add(void* manager_raw, int client_fd) {
  manager_t* manager = (manager_t*) manager_raw;

  server_clients_add(manager->clients, client_fd);
}

void manager_on_client_event(void* manager_raw, int client_fd) {
  manager_t* manager = (manager_t*) manager_raw;

  svr_client_t* client = server_clients_get(manager->clients, client_fd);

  while (true) {
    svr_client_result_t res = server_process_client_until_result(manager->server, client);

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE) {
      server_rearm_client_to_epoll(manager->server, client->fd, true, false);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
      server_rearm_client_to_epoll(manager->server, client->fd, false, true);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
      clients_awaiting_flush_append(manager->clients_awaiting_flush, client);
      break;
    }

    if (res == SVR_CLIENT_RESULT_END) {
      client->method = METHOD__UNKNOWN;
      if (client->args_parser != NULL) {
        server_method_args_parser_destroy(client->args_parser);
        client->args_parser = NULL;
      }
      if (client->method_state != NULL) {
        client->method_state_destructor(client->method_state);
        client->method_state = NULL;
        client->method_state_destructor = NULL;
      }
      continue;
    }

    if (res == SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR) {
      server_clients_close(manager->clients, client);
      break;
    }

    fprintf(stderr, "Unknown client (method=%d) action result: %d\n", client->method, res);
    exit(EXIT_INTERNAL);
  }
}

manager_t* manager_create(
  buckets_t* bkts,
  device_t* dev,
  flush_state_t* flush_state,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  stream_t* stream
) {
  server_methods_t* methods = server_methods_create();
  server_methods_add(methods, METHOD_COMMIT_OBJECT, method_commit_object_state_create, method_commit_object, method_commit_object_state_destroy);
  server_methods_add(methods, METHOD_CREATE_OBJECT, method_create_object_state_create, method_create_object, method_create_object_state_destroy);
  server_methods_add(methods, METHOD_DELETE_OBJECT, method_delete_object_state_create, method_delete_object, method_delete_object_state_destroy);

  manager_t* mgr = malloc(sizeof(manager_t));

  mgr->clients = server_clients_create();
  mgr->server = server_create(
    MANAGER_SOCK_PATH,
    mgr,
    manager_on_client_add,
    manager_on_client_event,
    &mgr->method_handler_ctx,
    methods
  );

  mgr->method_handler_ctx.bkts = bkts;
  mgr->method_handler_ctx.dev = dev;
  mgr->method_handler_ctx.flush_state = flush_state;
  mgr->method_handler_ctx.fl = fl;
  mgr->method_handler_ctx.inodes_state = inodes_state;
  mgr->method_handler_ctx.stream = stream;

  mgr->flush_state = flush_state;
  if (-1 == clock_gettime(CLOCK_MONOTONIC, &mgr->last_flushed)) {
    perror("Failed to get current time");
    exit(EXIT_INTERNAL);
  }
  mgr->clients_awaiting_flush = clients_awaiting_flush_create();
  return mgr;
}

void* manager_thread(void* mgr_raw) {
  manager_t* mgr = (manager_t*) mgr_raw;

  ts_log(INFO, "Started manager");

  while (true) {
    // Aim to flush every 100 ms.
    server_wait_epoll(mgr->server, 100);

    for (uint64_t i = 0; i < mgr->clients_awaiting_flush->len; i++) {
      svr_client_t* client = mgr->clients_awaiting_flush->elems[i];
      // NOTE: It's possible that the client hasn't written anything, so don't add to epoll as that could cause infinite wait.
      manager_on_client_event(mgr, client->fd);
    }
    mgr->clients_awaiting_flush->len = 0;
  }
}

void* manager_start(manager_t* mgr) {
  pthread_t* t = malloc(sizeof(pthread_t));
  ASSERT_ERROR_RETVAL_OK(pthread_create(t, NULL, manager_thread, mgr), "start manager");
  return t;
}

void manager_join(void* handle) {
  pthread_t* t = (pthread_t*) handle;
  ASSERT_ERROR_RETVAL_OK(pthread_join(*t, NULL), "join manager");
  free(t);
}
