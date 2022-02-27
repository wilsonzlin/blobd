#pragma once

#include <pthread.h>

typedef struct {
  pthread_rwlock_t rwlock;
} flush_state_t;

flush_state_t* flush_state_create();
