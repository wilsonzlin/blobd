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

LOGGER("manager");

LIST_DEF(clients_awaiting_flush, svr_client_t*);
LIST(clients_awaiting_flush, svr_client_t*);

struct manager_s {
  manager_state_t* manager_state;
  flush_state_t* flush_state;
  server_t* server;
  struct timespec last_flushed;
  clients_awaiting_flush_t* clients_awaiting_flush;
};

manager_t* manager_create(manager_state_t* manager_state, flush_state_t* flush_state, server_t* server) {
  manager_t* mgr = malloc(sizeof(manager_t));
  mgr->manager_state = manager_state;
  mgr->flush_state = flush_state;
  mgr->server = server;
  if (-1 == clock_gettime(CLOCK_MONOTONIC, &mgr->last_flushed)) {
    perror("Failed to get current time");
    exit(EXIT_INTERNAL);
  }
  mgr->clients_awaiting_flush = clients_awaiting_flush_create();
  return mgr;
}

void* thread(void* mgr_raw) {
  manager_t* mgr = (manager_t*) mgr_raw;

  ts_log(INFO, "Started manager worker");

  while (true) {
    if (mgr->clients_awaiting_flush->len) {
      struct timespec now;
      if (-1 == clock_gettime(CLOCK_MONOTONIC, &now)) {
        perror("Failed to get current time");
        exit(EXIT_INTERNAL);
      }
      // Aim to flush every 100 ms.
      if (now.tv_sec - mgr->last_flushed.tv_sec > 0 || now.tv_nsec - mgr->last_flushed.tv_nsec > 100 * 1000 * 1000) {
        flush_perform(mgr->flush_state);
        if (-1 == clock_gettime(CLOCK_MONOTONIC, &mgr->last_flushed)) {
          perror("Failed to get current time");
          exit(EXIT_INTERNAL);
        }
        for (uint64_t i = 0; i < mgr->clients_awaiting_flush->len; i++) {
          svr_client_t* client = mgr->clients_awaiting_flush->elems[i];
          server_hand_back_client_from_manager(mgr->server, client, SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE);
        }
        mgr->clients_awaiting_flush->len = 0;
        continue;
      }
    }

    svr_client_t* client = (svr_client_t*) lossy_mpsc_queue_dequeue(mgr->manager_state->client_queue);
    if (client == NULL) {
      // Let's sleep for 2 ms.
      struct timespec sleep_req;
      sleep_req.tv_sec = 0;
      sleep_req.tv_nsec = 2 * 1000 * 1000;
      if (-1 == nanosleep(&sleep_req, NULL)) {
        perror("Failed to sleep flushing worker");
        exit(EXIT_INTERNAL);
      }
      continue;
    }

    svr_method_handler_ctx_t* ctx = server_get_method_handler_context(mgr->server);
    method_t method = client->method;
    void* method_state = client->method_state;
    int fd = client->fd;
    svr_client_result_t res;
    if (method == SVR_METHOD_CREATE_OBJECT) {
      res = method_create_object(ctx, method_state, fd);
    } else if (method == SVR_METHOD_COMMIT_OBJECT) {
      res = method_commit_object(ctx, method_state, fd);
    } else if (method == SVR_METHOD_DELETE_OBJECT) {
      res = method_delete_object(ctx, method_state, fd);
    } else {
      fprintf(stderr, "Unknown client method %u\n", method);
      exit(EXIT_INTERNAL);
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
      clients_awaiting_flush_append(mgr->clients_awaiting_flush, client);
    } else {
      server_hand_back_client_from_manager(mgr->server, client, res);
    }
  }
}

void manager_start(manager_t* mgr) {
  pthread_t t;
  ASSERT_ERROR_RETVAL_OK(pthread_create(&t, NULL, thread, mgr), "start manager worker thread");
}
