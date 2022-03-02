#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include "device.h"
#include "list.h"
#include "stream.h"

LIST(events_pending_flush, stream_event_t);

stream_t* stream_create_from_device(device_t* dev, uint64_t dev_offset) {
  stream_t* st = malloc(sizeof(stream_t));
  st->dev = dev;
  st->dev_offset = dev_offset;
  uint64_t next_obj_no = read_u64(dev->mmap + dev_offset);
  uint64_t next_seq_no = read_u64(dev->mmap + dev_offset + 8);
  atomic_init(&st->next_seq_no, next_seq_no);
  st->pending_flush = events_pending_flush_create();
  return st;
}

uint64_t stream_acquire_obj_no(
  stream_t* stream
) {
  return atomic_fetch_add_explicit(&stream->next_obj_no, 1, memory_order_relaxed);
}

uint64_t stream_acquire_seq_no(
  stream_t* stream,
  stream_event_type_t typ,
  uint64_t bkt_id,
  uint64_t obj_no
) {
  uint64_t seq_no = atomic_fetch_add_explicit(&stream->next_seq_no, 1, memory_order_relaxed);
  stream_event_t e = {
    .seq_no = seq_no,
    .typ = typ,
    .bkt_id = bkt_id,
    .obj_no = obj_no,
  };
  events_pending_flush_append(stream->pending_flush, e);
  return seq_no;
}
