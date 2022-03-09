#define _GNU_SOURCE

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
  server_t* server;
  worker_method_handler_ctx_t ctx;
};

void worker_on_client_event(void* worker_raw, svr_client_t* client) {
  worker_t* worker = (worker_t*) worker_raw;

  while (true) {
    svr_client_result_t res = server_process_client_until_result(worker->server, client);

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE) {
      server_rearm_client_to_epoll(worker->server, client, true, false);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
      server_rearm_client_to_epoll(worker->server, client, false, true);
      break;
    }

    if (res == SVR_CLIENT_RESULT_END) {
      server_client_reset(client);
      continue;
    }

    if (res == SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR) {
      server_close_client(worker->server, client);
      break;
    }

    fprintf(stderr, "Unknown client (method=%d) action result: %d\n", server_client_get_method(client), res);
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
  worker->server = server_create(
    WORKER_SOCK_PATH,
    worker,
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
