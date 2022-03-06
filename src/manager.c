#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "exit.h"
#include "flush.h"
#include "list.h"
#include "log.h"
#include "manager.h"
#include "manager_state.h"
#include "method/_common.h"
#include "method/commit_object.h"
#include "method/create_object.h"
#include "method/delete_object.h"
#include "server.h"

#define MANAGER_SOCK_PATH "/tmp/turbostore-manager.sock"

LOGGER("manager");

LIST_DEF(clients_awaiting_flush, svr_client_t*);
LIST(clients_awaiting_flush, svr_client_t*);

KHASH_MAP_INIT_INT(fd_to_client, svr_client_t*);

struct manager_s {
  kh_fd_to_client_t* fd_to_client;
  server_t* server;

  manager_method_handler_ctx_t method_handler_ctx;

  flush_state_t* flush_state;
  struct timespec last_flushed;
  clients_awaiting_flush_t* clients_awaiting_flush;
};

#define READ_OR_RELEASE(readres, fd, buf, n) \
  readres = maybe_read(fd, buf, n); \
  if (!readres) { \
    return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE; \
  } \
  if (readres < 0) { \
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
  }

void manager_on_client_add(void* manager_raw, int client_fd) {
  manager_t* manager = (manager_t*) manager_raw;

  svr_client_t* client = malloc(sizeof(svr_client_t));
  client->fd = client_fd;
  client->args_parser = NULL;
  client->method = METHOD__UNKNOWN;
  client->method_state = NULL;

  // Map FD to client ID.
  int kh_res;
  khint_t kh_it = kh_put_svr_fd_to_client(manager->fd_to_client, client_fd, &kh_res);
  if (-1 == kh_res) {
    fprintf(stderr, "Failed to insert client into map\n");
    exit(EXIT_INTERNAL);
  }
  kh_val(manager->fd_to_client, kh_it) = client;
}

void manager_on_client_event(void* manager_raw, int client_fd) {
  manager_t* manager = (manager_t*) manager_raw;

  khint_t kh_it = kh_get_svr_fd_to_client(manager->fd_to_client, client_fd);
  if (kh_it == kh_end(manager->fd_to_client)) {
    fprintf(stderr, "Client does not exist\n");
    exit(EXIT_INTERNAL);
  }
  svr_client_t* client = kh_val(manager->fd_to_client, kh_it);

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

    if (res == SVR_CLIENT_RESULT_END) {
      res = SVR_CLIENT_RESULT__UNKNOWN;
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
      if (-1 == close(client->fd)) {
        perror("Failed to close client FD");
        exit(EXIT_INTERNAL);
      }
      khint_t k = kh_get_svr_fd_to_client(manager->fd_to_client, client->fd);
      if (k == kh_end(manager->fd_to_client)) {
        fprintf(stderr, "Client does not exist\n");
        exit(EXIT_INTERNAL);
      }
      kh_del_svr_fd_to_client(manager->fd_to_client, k);
      // Destroy the client.
      if (client->args_parser != NULL) {
        server_method_args_parser_destroy(client->args_parser);
      }
      if (client->method_state != NULL) {
        client->method_state_destructor(client->method_state);
      }
      free(client);
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

  mgr->fd_to_client = kh_init_fd_to_client();
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

    if (mgr->clients_awaiting_flush->len) {
      struct timespec now;
      if (-1 == clock_gettime(CLOCK_MONOTONIC, &now)) {
        perror("Failed to get current time");
        exit(EXIT_INTERNAL);
      }
      if (now.tv_sec - mgr->last_flushed.tv_sec > 0 || now.tv_nsec - mgr->last_flushed.tv_nsec > 100 * 1000 * 1000) {
        flush_perform(mgr->flush_state);
        if (-1 == clock_gettime(CLOCK_MONOTONIC, &mgr->last_flushed)) {
          perror("Failed to get current time");
          exit(EXIT_INTERNAL);
        }
        for (uint64_t i = 0; i < mgr->clients_awaiting_flush->len; i++) {
          svr_client_t* client = mgr->clients_awaiting_flush->elems[i];
          server_rearm_client_to_epoll(mgr->server, client->fd, false, true);
        }
        mgr->clients_awaiting_flush->len = 0;
      }
    }
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
