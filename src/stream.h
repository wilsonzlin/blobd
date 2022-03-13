#pragma once

#include <pthread.h>
#include <stdint.h>
#include "device.h"
#include "list.h"

/**

STREAM
======

Sequence number 0 and object number 0 are reserved.

For replicas, next_seq_no contains the next unsynchronised event. If the upstream no longer has the record, the replica has fallen too far behind and a full device clone of the upstream or another up-to-date replica needs to be done.

There may be gaps in object numbers (i.e. some may appear to never be used). However, there must never be any gaps in sequence numbers; all events must follow the previous event's sequence number sequentially and never skip any.

The event with sequence number N is in the ring buffer at index (N % STREAM_EVENTS_BUF_LEN). However, if the element does not have a matching sequence number in the "seq_no" field, it means either the event doesn't exist (if value is less) or is too old and has already been evicted (if value is greater).

Structure
---------

u64 next_obj_no
u64 next_seq_no
{
  u8 type
  u40 bucket_id
  u64 seq_no
  u64 obj_no
}[STREAM_EVENTS_BUF_LEN] events_ring_buffer

**/

#define STREAM_EVENTS_BUF_LEN 16777216

#define STREAM_EVENT_OFFSETOF_TYPE 0
#define STREAM_EVENT_OFFSETOF_BUCKET_ID (STREAM_EVENT_OFFSETOF_TYPE + 1)
#define STREAM_EVENT_OFFSETOF_SEQ_NO (STREAM_EVENT_OFFSETOF_BUCKET_ID + 5)
#define STREAM_EVENT_OFFSETOF_OBJ_NO (STREAM_EVENT_OFFSETOF_SEQ_NO + 8)

#define STREAM_OFFSETOF_NEXT_OBJ_NO 0
#define STREAM_OFFSETOF_NEXT_SEQ_NO (STREAM_OFFSETOF_NEXT_OBJ_NO + 8)
#define STREAM_OFFSETOF_EVENT(idx) (STREAM_OFFSETOF_NEXT_SEQ_NO + 8 + (idx) * (STREAM_EVENT_OFFSETOF_OBJ_NO + 8))

#define STREAM_RESERVED_SPACE STREAM_OFFSETOF_EVENT(STREAM_EVENTS_BUF_LEN)

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

typedef struct {
  device_t* dev;
  uint64_t dev_offset;
  // For reading and writing events only.
  pthread_rwlock_t rwlock;
  _Atomic(uint64_t) next_obj_no;
  // This does not need to be atomic as we use external synchronisation.
  uint64_t next_seq_no;
} stream_t;

stream_t* stream_create_from_device(device_t* dev, uint64_t dev_offset);
