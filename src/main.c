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
  if (argc != 5) {
    fprintf(stderr, "Not enough arguments provided\n");
    exit(EXIT_CONF);
  }

  char* arg_action = argv[1];
  char* arg_dev = argv[2];
  char* arg_worker_count = argv[3];
  char* arg_client_max = argv[4];

  int dev_fd = open(arg_dev, O_RDWR);
  if (-1 == dev_fd) {
    perror("Failed to open block device");
    exit(EXIT_CONF);
  }

  size_t dev_size;
  if (-1 == ioctl(dev_fd, BLKGETSIZE64, &dev_size)) {
    perror("Failed to get block device size");
    exit(EXIT_INTERNAL);
  }

  char* dev_mmap = mmap(NULL, dev_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, dev_fd, 0);
  if (MAP_FAILED == dev_mmap) {
    perror("Failed to map block device");
    exit(EXIT_INTERNAL);
  }

  if (-1 == close(dev_fd)) {
    perror("Failed to close block device file descriptor");
    exit(EXIT_INTERNAL);
  }

  if (!strcmp("format", arg_action)) {
    // TODO
  }

  if (strcmp("start", arg_action)) {
    fprintf(stderr, "Unknown action: %s\n", arg_action);
    exit(EXIT_CONF);
  }

  errno = 0;
  size_t worker_count = strtoull(arg_worker_count, NULL, 10);
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

  errno = 0;
  size_t client_max_log2 = strtoull(arg_client_max, NULL, 10);
  if (errno != 0) {
    perror("Failed to parse maximum clients argument");
    exit(EXIT_CONF);
  }

  svr_clients_t* svr = server_clients_create(client_max_log2);

  device_t* dev = device_create(dev_mmap, dev_size);

  journal_t* journal = journal_create(0);

  freelist_t* freelist = freelist_create_from_disk_state(dev, JOURNAL_RESERVED_SPACE);

  buckets_t* buckets = buckets_create_from_disk_state(dev, JOURNAL_RESERVED_SPACE + 2097152 * (1 + 3 * 8 + 8));

  flush_state_t* flush = flush_create();

  flush_worker_start(
    flush,
    svr,
    dev,
    journal,
    freelist,
    buckets
  );

  server_start_loop(
    svr,
    worker_count,
    dev,
    flush,
    freelist,
    buckets
  );

  return 0;
}
