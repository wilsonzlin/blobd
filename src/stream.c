#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include "device.h"
#include "list.h"
#include "log.h"
#include "stream.h"
#include "util.h"

LOGGER("stream");

stream_t* stream_create_from_device(device_t* dev, uint64_t dev_offset) {
  stream_t* st;
  ASSERT_ERROR_RETVAL_OK(posix_memalign((void**) &st, 16, sizeof(stream_t)), "alloc stream");
  st->dev = dev;
  st->dev_offset = dev_offset;
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&st->rwlock, NULL), "init stream lock");

  uint64_t next_obj_no = read_u64(dev->mmap + dev_offset);
  ts_log(INFO, "Next object number is %lu", next_obj_no);
  atomic_init(&st->next_obj_no, next_obj_no);

  uint64_t next_seq_no = read_u64(dev->mmap + dev_offset + 8);
  ts_log(INFO, "Next sequence number is %lu", next_seq_no);
  st->next_seq_no = next_seq_no;

  return st;
}
