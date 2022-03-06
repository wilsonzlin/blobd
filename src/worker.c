#define _GNU_SOURCE

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include "errno.h"
#include "exit.h"
#include "flush.h"
#include "log.h"
#include "manager.h"
#include "method/_common.h"
#include "method/inspect_object.h"
#include "method/read_object.h"
#include "method/write_object.h"
#include "server_client.h"
#include "server_method_args.h"
#include "server.h"
#include "util.h"
#include "worker.h"

#define WORKER_SOCK_PATH "/tmp/turbostore.sock"

LOGGER("worker");

struct worker_s {
  pthread_rwlock_t fd_to_client_lock;
  server_t* server;
  server_clients_t* clients;
  worker_method_handler_ctx_t ctx;
};

void worker_on_client_add(void* worker_raw, int client_fd) {
  worker_t* worker = (worker_t*) worker_raw;

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&worker->fd_to_client_lock), "acquire write lock on FD map");
  server_clients_add(worker->clients, client_fd);
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&worker->fd_to_client_lock), "release write lock on FD map");
}

void worker_on_client_event(void* worker_raw, int client_fd) {
  worker_t* worker = (worker_t*) worker_raw;

  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&worker->fd_to_client_lock), "acquire read lock on FD map");
  svr_client_t* client = server_clients_get(worker->clients, client_fd);
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&worker->fd_to_client_lock), "release read lock on FD map");

  while (true) {
    svr_client_result_t res = server_process_client_until_result(worker->server, client);

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE) {
      server_rearm_client_to_epoll(worker->server, client->fd, true, false);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
      server_rearm_client_to_epoll(worker->server, client->fd, false, true);
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
      ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&worker->fd_to_client_lock), "acquire write lock on FD map");
      // Close before removing from map as otherwise an epoll event might still emit,
      // but acquire lock before closing in case FD is reused immediately.
      server_clients_close(worker->clients, client);
      ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&worker->fd_to_client_lock), "release write lock on FD map");
      break;
    }

    fprintf(stderr, "Unknown client (method=%d) action result: %d\n", client->method, res);
    exit(EXIT_INTERNAL);
  }
}

worker_t* worker_create(
  device_t* dev,
  buckets_t* bkts
) {
  server_methods_t* methods = server_methods_create();
  server_methods_add(methods, METHOD_INSPECT_OBJECT, method_inspect_object_state_create, method_inspect_object, method_inspect_object_state_destroy);
  server_methods_add(methods, METHOD_READ_OBJECT, method_read_object_state_create, method_read_object, method_read_object_state_destroy);
  server_methods_add(methods, METHOD_WRITE_OBJECT, method_write_object_state_create, method_write_object, method_write_object_state_destroy);

  worker_t* worker = malloc(sizeof(worker_t));
  worker->clients = server_clients_create();
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&worker->fd_to_client_lock, NULL), "create lock for FD map");
  worker->server = server_create(
    WORKER_SOCK_PATH,
    worker,
    worker_on_client_add,
    worker_on_client_event,
    &worker->ctx,
    methods
  );
  worker->ctx.bkts = bkts;
  worker->ctx.dev = dev;
  return worker;
}

void* worker_thread(void* worker_raw) {
  worker_t* worker = (worker_t*) worker_raw;

  while (true) {
    server_wait_epoll(worker->server, -1);
  }

  return NULL;
}

void* workers_start(
  worker_t* worker,
  uint64_t count
) {
  pthread_t* threads = malloc(sizeof(pthread_t) * count);
  for (uint64_t i = 0; i < count; i++) {
    ASSERT_ERROR_RETVAL_OK(pthread_create(&threads[i], NULL, worker_thread, worker), "create worker");
  }
  return threads;
}

void workers_join(void* handle_raw, uint64_t worker_count) {
  pthread_t* threads = (pthread_t*) handle_raw;

  for (uint64_t i = 0; i < worker_count; i++) {
    ASSERT_ERROR_RETVAL_OK(pthread_join(threads[i], NULL), "join worker");
  }

  free(threads);
}
