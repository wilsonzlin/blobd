#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include "device.h"
#include "list.h"
#include "log.h"
#include "stream.h"

LOGGER("stream");

LIST(events_pending_flush, stream_event_t);

stream_t* stream_create_from_device(device_t* dev, uint64_t dev_offset) {
  stream_t* st = aligned_alloc(16, sizeof(stream_t));
  st->dev = dev;
  st->dev_offset = dev_offset;
  uint64_t next_obj_no = read_u64(dev->mmap + dev_offset);
  ts_log(DEBUG, "Next object number is %lu", next_obj_no);
  atomic_init(&st->next_obj_no, next_obj_no);
  uint64_t next_seq_no = read_u64(dev->mmap + dev_offset + 8);
  ts_log(DEBUG, "Next sequence number is %lu", next_seq_no);
  atomic_init(&st->next_seq_no, next_seq_no);
  st->pending_flush = events_pending_flush_create();
  return st;
}
