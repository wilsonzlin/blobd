#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include "errno.h"
#include "exit.h"
#include "log.h"
#include "server.h"
#include "util.h"
#include "worker.h"

LOGGER("worker");

void* worker_thread(void* server_raw) {
  server_t* server = (server_t*) server_raw;

  while (true) {
    server_wait_epoll(server, -1);
  }

  return NULL;
}

void* workers_start(
  server_t* server,
  uint64_t count
) {
  pthread_t* threads = malloc(sizeof(pthread_t) * count);
  for (uint64_t i = 0; i < count; i++) {
    ASSERT_ERROR_RETVAL_OK(pthread_create(&threads[i], NULL, worker_thread, server), "create worker");
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
