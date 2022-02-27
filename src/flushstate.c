#define _GNU_SOURCE

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include "exit.h"
#include "flushstate.h"

flush_state_t* flush_state_create() {
  flush_state_t* flush = malloc(sizeof(flush_state_t));
  if (pthread_rwlock_init(&flush->rwlock, NULL)) {
    perror("Failed to create flushing lock");
    exit(EXIT_INTERNAL);
  }
  return flush;
}
