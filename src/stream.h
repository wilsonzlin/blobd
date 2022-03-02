#pragma once

/**

STREAM
======

Structure
---------

u8 replica_no_or_zero_if_primary
u128 next_seq_no
{
  u128 last_known_seq_no
}[255] replica_state
u128 events_base_seq_no
u32 events_ring_buffer_start
{
  u8 type
  u64 bucket_id
  u128 object_seq_no
}[STREAM_EVENTS_BUF_LEN] events_ring_buffer

**/

#define STREAM_EVENTS_BUF_LEN 10000000

typedef enum {
  STREAM_EVENT__NONE = 0,
  STREAM_EVENT_OBJECT_CREATE = 1,
  STREAM_EVENT_OBJECT_COMMIT = 2,
  STREAM_EVENT_OBJECT_DELETE = 3,
} stream_event_type_t;
