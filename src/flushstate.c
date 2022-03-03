#define _GNU_SOURCE

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include "exit.h"
#include "flushstate.h"
#include "log.h"
#include "util.h"

LOGGER("flushstate");

flush_state_t* flush_state_create() {
  flush_state_t* flush = malloc(sizeof(flush_state_t));
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&flush->rwlock, NULL), "create flushing lock");
  return flush;
}
