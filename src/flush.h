#pragma once

#include "bucket.h"
#include "device.h"
#include "flushstate.h"
#include "freelist.h"
#include "journal.h"
#include "server.h"
#include "stream.h"

void flush_worker_start(
  flush_state_t* flush,
  server_t* svr,
  device_t* dev,
  journal_t* journal,
  freelist_t* fl,
  buckets_t* buckets,
  stream_t* stream
);
