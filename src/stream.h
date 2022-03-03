#pragma once

#include <stdatomic.h>
#include <stdint.h>
#include "device.h"
#include "list.h"

/**

STREAM
======

Sequence number 0 and object number 0 are reserved.

For replicas, next_seq_no contains the next unsynchronised event. If the upstream no longer has the record, the replica has fallen too far behind and a full device clone of the upstream or another up-to-date replica needs to be done.

Structure
---------

u64 next_obj_no
u64 next_seq_no
{
  u8 type
  u40 bucket_id
  u64 obj_no
}[STREAM_EVENTS_BUF_LEN] events_ring_buffer

**/

#define STREAM_EVENTS_BUF_LEN 16777216

#define STREAM_RESERVED_SPACE (8 + 8 + (STREAM_EVENTS_BUF_LEN) * (1 + 5 + 8))

typedef enum {
  STREAM_EVENT__NONE = 0,
  STREAM_EVENT_OBJECT_COMMIT = 1,
  STREAM_EVENT_OBJECT_DELETE = 2,
} stream_event_type_t;

typedef struct {
  uint64_t seq_no;
  stream_event_type_t typ;
  uint64_t bkt_id;
  uint64_t obj_no;
} stream_event_t;

LIST_DEF(events_pending_flush, stream_event_t);

typedef struct {
  device_t* dev;
  uint64_t dev_offset;
  atomic_uint_least64_t next_obj_no __attribute__((aligned (16)));
  atomic_uint_least64_t next_seq_no __attribute__((aligned (16)));
  events_pending_flush_t* pending_flush;
} stream_t;

stream_t* stream_create_from_device(device_t* dev, uint64_t dev_offset);
