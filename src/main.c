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
#include "conf.h"
#include "device.h"
#include "exit.h"
#include "flush.h"
#include "freelist.h"
#include "inode.h"
#include "log.h"
#include "tile.h"
#include "util.h"
#include "worker.h"

LOGGER("main");

#define CONF_LEN_MAX 65536

int main(int argc, char** argv) {
  if (argc != 3) {
    fprintf(stderr, "Not enough or too many arguments provided\n");
    exit(EXIT_CONF);
  }

  char* arg_action = argv[1];
  char* arg_conf = argv[2];

  long page_size = sysconf(_SC_PAGESIZE);
  if (-1 == page_size) {
    perror("Failed to get system page size");
    exit(EXIT_INTERNAL);
  }

  conf_parser_t* conf_parser = conf_parser_create();

  char* conf_raw = malloc(CONF_LEN_MAX + 1);
  int conf_fd = open(arg_conf, O_RDONLY);
  int conf_len = read(conf_fd, conf_raw, CONF_LEN_MAX + 1);
  if (conf_len < 0) {
    perror("Failed to read conf file");
    exit(EXIT_CONF);
  }
  if (conf_len == CONF_LEN_MAX) {
    fprintf(stderr, "Conf file is too large\n");
    exit(EXIT_CONF);
  }
  if (-1 == close(conf_fd)) {
    perror("Failed to close conf file");
    exit(EXIT_INTERNAL);
  }

  conf_t* conf = conf_parse(conf_parser, conf_raw, conf_len);
  free(conf_raw);
  conf_parser_destroy(conf_parser);

  char* arg_dev = conf->device_path;
  if (arg_dev == NULL) {
    fprintf(stderr, "Device path was not specified\n");
    exit(EXIT_CONF);
  }

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

  ts_log(INFO, "Block device %s has size %lu", arg_dev, dev_size);

  // Do not use MAP_PRIVATE as that requires reserving memory upfront.
  char* dev_mmap = mmap(NULL, dev_size, PROT_READ | PROT_WRITE, MAP_SHARED_VALIDATE, dev_fd, 0);
  if (MAP_FAILED == dev_mmap) {
    perror("Failed to map block device");
    exit(EXIT_INTERNAL);
  }

  device_t* dev = device_create(dev_fd, dev_mmap, dev_size, page_size);

  if (!strcmp("format", arg_action)) {
    uint64_t bucket_count = conf->bucket_count;

    if (bucket_count & (bucket_count - 1)) {
      fprintf(stderr, "Bucket count must be a power of 2\n");
      exit(EXIT_CONF);
    }

    if (bucket_count < 4096llu || bucket_count > 281474976710656llu) {
      fprintf(stderr, "Bucket count must be in the range [4096, 281474976710656]\n");
      exit(EXIT_CONF);
    }

    uint64_t bucket_count_log2 = _tzcnt_u64(bucket_count);

    device_format(dev, bucket_count_log2);
  } else if (!strcmp("start", arg_action)) {
    uint64_t worker_count = conf->worker_threads;

    if (worker_count == 0) {
      int v = sysconf(_SC_NPROCESSORS_ONLN);
      if (-1 == v) {
        perror("Failed to get online CPU count");
        exit(EXIT_INTERNAL);
      }
      worker_count = v;
    }

    uint64_t journal_dev_offset = 0;
    uint64_t stream_dev_offset = JOURNAL_RESERVED_SPACE;
    uint64_t freelist_dev_offset = stream_dev_offset + STREAM_RESERVED_SPACE;
    uint64_t buckets_dev_offset = freelist_dev_offset + FREELIST_RESERVED_SPACE;

    journal_t* journal = journal_create(
      dev,
      journal_dev_offset,
      freelist_dev_offset,
      buckets_read_count(dev, buckets_dev_offset) - 1,
      freelist_dev_offset,
      stream_dev_offset
    );

    journal_apply_offline_then_clear(journal);

    stream_t* stream = stream_create_from_device(dev, stream_dev_offset);

    freelist_t* freelist = freelist_create_from_disk_state(dev, freelist_dev_offset);

    buckets_t* buckets = buckets_create_from_disk_state(dev, buckets_dev_offset);

    method_ctx_t* method_ctx = malloc(sizeof(method_ctx_t));

    server_t* server = server_create(
      conf->worker_address,
      conf->worker_port,
      conf->worker_unix_socket_path,
      method_ctx
    );

    flush_state_t* flush_state = flush_state_create(buckets, dev, freelist, journal, server, stream);

    method_ctx->bkts = buckets;
    method_ctx->dev = dev;
    method_ctx->fl = freelist;
    method_ctx->flush_state = flush_state;
    method_ctx->stream = stream;

    void* flush_handle = flush_worker_start(flush_state);

    void* workers_handle = workers_start(server, worker_count);

    workers_join(workers_handle, worker_count);

    flush_worker_join(flush_handle);
  } else {
    fprintf(stderr, "Unknown action: %s\n", arg_action);
    exit(EXIT_CONF);
  }

  conf_destroy(conf);

  return 0;
}
