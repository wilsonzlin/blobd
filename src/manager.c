#include "exit.h"
#include "flush.h"
#include "list.h"
#include "log.h"
#include "lossy_mpsc_queue.h"
#include "manager.h"
#include "server.h"

LOGGER("manager");

typedef struct {
  int fd;
  svr_method_t method;
  void* method_state;
} client_t;

struct manager_state_s {
  lossy_mpsc_queue_t* client_queue;
};

manager_state_t* manager_state_create() {
  manager_state_t* st = malloc(sizeof(manager_state_t));
  st->client_queue = lossy_mpsc_queue_create(20);
  return st;
}

LIST_DEF(clients_awaiting_flush, client_t*);
LIST(clients_awaiting_flush, client_t*);

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

void* thread(manager_t* mgr) {
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
        continue;
      }
    }

    client_t* client = (client_t*) lossy_mpsc_queue_dequeue(mgr->manager_state->client_queue);
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

    // TODO
  }
}

void manager_start(manager_t* mgr) {
  pthread_t t;
  ASSERT_ERROR_RETVAL_OK(pthread_create(&t, NULL, thread, mgr), "start manager worker thread");
}
