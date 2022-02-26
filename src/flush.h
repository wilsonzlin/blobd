#pragma once

#include <pthread.h>
#include "bucket.h"
#include "device.h"
#include "freelist.h"
#include "journal.h"

typedef struct {
  pthread_rwlock_t rwlock;
} flush_state_t;

flush_state_t* flush_create();

void flush_worker_start(
  flush_state_t* flush,
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  buckets_t* buckets
);
