#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "device.h"
#include "exit.h"
#include "flush.h"
#include "flushstate.h"
#include "freelist.h"
#include "server.h"
#include "tile.h"
#include "util.h"

int main(int argc, char** argv) {
  if (argc != 4) {
    fprintf(stderr, "Not enough arguments provided\n");
    exit(EXIT_CONF);
  }

  char* arg_action = argv[1];
  char* arg_dev = argv[2];
  char* arg_worker_or_bucket_count = argv[3];

  int dev_fd = open(arg_dev, O_RDWR);
  if (-1 == dev_fd) {
    perror("Failed to open block device");
    exit(EXIT_CONF);
  }

  uint64_t dev_size;
  if (-1 == ioctl(dev_fd, BLKGETSIZE64, &dev_size)) {
    perror("Failed to get block device size");
    exit(EXIT_INTERNAL);
  }

  // Do not use MAP_PRIVATE as that requires reserving memory upfront.
  char* dev_mmap = mmap(NULL, dev_size, PROT_READ | PROT_WRITE, MAP_SHARED_VALIDATE, dev_fd, 0);
  if (MAP_FAILED == dev_mmap) {
    perror("Failed to map block device");
    exit(EXIT_INTERNAL);
  }

  if (-1 == close(dev_fd)) {
    perror("Failed to close block device file descriptor");
    exit(EXIT_INTERNAL);
  }

  device_t* dev = device_create(dev_mmap, dev_size);

  if (!strcmp("format", arg_action)) {
    errno = 0;
    uint64_t bucket_count = strtoull(arg_worker_or_bucket_count, NULL, 10);
    if (errno != 0) {
      perror("Failed to parse bucket count argument");
      exit(EXIT_CONF);
    }

    if (bucket_count & (bucket_count - 1)) {
      fprintf(stderr, "Bucket count must be a power of 2\n");
      exit(EXIT_CONF);
    }

    if (bucket_count < 4096 || bucket_count > 281474976710656) {
      fprintf(stderr, "Bucket count must be in the range [4096, 281474976710656]\n");
      exit(EXIT_CONF);
    }

    uint64_t bucket_count_log2 = _tzcnt_u64(bucket_count);

    device_format(dev, bucket_count_log2);

    return 0;
  }

  if (strcmp("start", arg_action)) {
    fprintf(stderr, "Unknown action: %s\n", arg_action);
    exit(EXIT_CONF);
  }

  errno = 0;
  uint64_t worker_count = strtoull(arg_worker_or_bucket_count, NULL, 10);
  if (errno != 0) {
    perror("Failed to parse worker count argument");
    exit(EXIT_CONF);
  }

  if (worker_count == 0) {
    int v = sysconf(_SC_NPROCESSORS_ONLN);
    if (-1 == v) {
      perror("Failed to get online CPU count");
      exit(EXIT_INTERNAL);
    }
    worker_count = v;
  }

  journal_t* journal = journal_create(dev, 0);

  stream_t* stream = stream_create_from_device(dev, JOURNAL_RESERVED_SPACE);

  freelist_t* freelist = freelist_create_from_disk_state(dev, JOURNAL_RESERVED_SPACE + STREAM_RESERVED_SPACE);

  buckets_t* buckets = buckets_create_from_disk_state(dev, JOURNAL_RESERVED_SPACE + STREAM_RESERVED_SPACE + FREELIST_RESERVED_SPACE);

  flush_state_t* flush = flush_state_create();

  server_t* svr = server_create(
    dev,
    flush,
    freelist,
    buckets
  );

  flush_worker_start(
    flush,
    svr,
    dev,
    journal,
    freelist,
    buckets,
    stream
  );

  server_start_loop(
    svr,
    worker_count
  );

  return 0;
}
