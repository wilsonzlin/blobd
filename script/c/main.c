#define _GNU_SOURCE

#include <endian.h>
#include <errno.h>
#include <immintrin.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define SOCKET_PATH_WORKER "/tmp/turbostore.sock"
#define SOCKET_PATH_MANAGER "/tmp/turbostore-manager.sock"
#define EPOLL_MAX_EVENTS 8192

int create_connection(char const* path) {
  int fd;
  if (-1 == (fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0))) {
    perror("Failed to open socket");
    exit(1);
  }

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
  if (-1 == connect(fd, (struct sockaddr*) &addr, sizeof(addr))) {
    perror("Failed to connect to socket");
    exit(1);
  }

  return fd;
}

void add_to_epoll(int epfd, int fd, void* data, int additional_events, int ctl) {
  struct epoll_event ev;
  ev.events = additional_events | EPOLLET | EPOLLONESHOT;
  ev.data.ptr = data;
  if (-1 == epoll_ctl(epfd, ctl, fd, &ev)) {
    perror("Failed to add to epoll");
    exit(1);
  }
}

typedef struct {
  // One of worker->{fd_manager,fd_worker}.
  int epollctl_fd;
  // One of {EPOLLIN, EPOLLOUT}, or zero to indicate end.
  int epollctl_events;
} worker_yield_t;

typedef enum {
  WS_INIT,
  WS_CREATE_OBJECT_WRITE,
  WS_CREATE_OBJECT_READ,
  WS_WRITE_OBJECT_WRITE_ARGS,
  WS_WRITE_OBJECT_WRITE_BODY,
  WS_WRITE_OBJECT_READ,
  WS_COMMIT_OBJECT_WRITE,
  WS_COMMIT_OBJECT_READ,
} worker_state_t;

typedef struct {
  worker_state_t state;
  uint64_t obj_no;
  uint8_t* key;
  uint8_t key_len;
  int fd_manager;
  int fd_worker;
  uint8_t* data;
  uint64_t size;
  _Atomic(uint64_t)* count_started;
  uint64_t count_total;
  // Reusable buffer for storing raw args data.
  uint8_t args_raw[255];
  // If we want to write bytes to a socket, we record state here.
  uint8_t* cur_write_data;
  uint64_t cur_write_completed;
  uint64_t cur_write_total;
  int cur_write_fd;
  // If we want to read bytes from a socket, we record state here.
  uint8_t cur_read_buf[255];
  uint64_t cur_read_completed;
  uint64_t cur_read_total;
  int cur_read_fd;
} worker_t;

static inline void worker_want_write(worker_t* w, uint8_t* data, uint64_t len, int fd) {
  w->cur_write_data = data;
  w->cur_write_completed = 0;
  w->cur_write_total = len;
  w->cur_write_fd = fd;
}

static inline void worker_want_read(worker_t* w, uint8_t len, int fd) {
  w->cur_read_completed = 0;
  w->cur_read_total = len;
  w->cur_read_fd = fd;
}

static inline void worker_detect_error(worker_t* w, char const* method) {
  if (w->cur_read_buf[0] != 0) {
    fprintf(stderr, "Method %s resulted in error %u\n", method, w->cur_read_buf[0]);
    exit(1);
  }
}

worker_yield_t worker(worker_t* w) {
  while (true) {
    if (w->cur_write_completed < w->cur_write_total) {
      // We must use `send` with MSG_NOSIGNAL over `write` in order to prevent SIGPIPE when trying to write to a closed connection.
      int writeno = send(w->cur_write_fd, w->cur_write_data, w->cur_write_total - w->cur_write_completed, MSG_NOSIGNAL);
      if (writeno > 0) {
        w->cur_write_completed += writeno;
        continue;
      }
      if (writeno == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return (worker_yield_t) {w->cur_write_fd, EPOLLOUT};
        }
      }
      // TODO This is not correct if writeno == 0.
      perror("Failed to write to socket");
      exit(1);
    }
    w->cur_write_total = 0;

    if (w->cur_read_completed < w->cur_read_total) {
      int readno = read(w->cur_read_fd, w->cur_read_buf, w->cur_read_total - w->cur_read_completed);
      if (readno > 0) {
        w->cur_read_completed += readno;
        continue;
      }
      if (readno == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return (worker_yield_t) {w->cur_read_fd, EPOLLIN};
        }
      }
      // TODO This is not correct if readno == 0.
      perror("Failed to read from socket");
      exit(1);
    }
    w->cur_read_total = 0;

    switch (w->state) {
    case WS_INIT:
      uint64_t id = atomic_fetch_add_explicit(w->count_started, 1, memory_order_relaxed);
      if (id >= w->count_total) {
        return (worker_yield_t) {0, 0};
      }
      w->key_len = sprintf((char*) w->key, "/random/data/%lu", id);
      w->state = WS_CREATE_OBJECT_WRITE;
      break;

    case WS_CREATE_OBJECT_WRITE:
      uint8_t co_len = 1 + w->key_len + 8;
      w->args_raw[0] = 1;
      w->args_raw[1] = co_len;
      w->args_raw[2] = w->key_len;
      memcpy(w->args_raw + 3, w->key, w->key_len);
      uint64_t co_arg_size = htobe64(w->size);
      memcpy(w->args_raw + 3 + w->key_len, &co_arg_size, 8);
      worker_want_write(w, w->args_raw, 2 + co_len, w->fd_manager);
      w->state = WS_CREATE_OBJECT_READ;
      worker_want_read(w, 9, w->fd_manager);
      break;

    case WS_CREATE_OBJECT_READ:
      worker_detect_error(w, "create_object");
      memcpy(&w->obj_no, w->cur_read_buf + 1, 8);
      w->obj_no = be64toh(w->obj_no);
      w->state = WS_WRITE_OBJECT_WRITE_ARGS;
      break;

    case WS_WRITE_OBJECT_WRITE_ARGS:
      uint8_t wr_len = 1 + w->key_len + 8 + 8;
      w->args_raw[0] = 4;
      w->args_raw[1] = wr_len;
      w->args_raw[2] = w->key_len;
      memcpy(w->args_raw + 3, w->key, w->key_len);
      uint64_t wr_arg_objno = htobe64(w->obj_no);
      memcpy(w->args_raw + 3 + w->key_len, &wr_arg_objno, 8);
      uint64_t wr_arg_start = htobe64(0);
      memcpy(w->args_raw + 3 + w->key_len + 8, &wr_arg_start, 8);
      worker_want_write(w, w->args_raw, 2 + wr_len, w->fd_worker);
      w->state = WS_WRITE_OBJECT_WRITE_BODY;
      break;

    case WS_WRITE_OBJECT_WRITE_BODY:
      // TODO We should really read response first; update once server method logic has changed.
      worker_want_write(w, w->data, w->size, w->fd_worker);
      w->state = WS_WRITE_OBJECT_READ;
      worker_want_read(w, 1, w->fd_worker);
      break;

    case WS_WRITE_OBJECT_READ:
      worker_detect_error(w, "write_object");
      w->state = WS_COMMIT_OBJECT_WRITE;
      break;

    case WS_COMMIT_OBJECT_WRITE:
      uint8_t cm_len = 1 + w->key_len + 8;
      w->args_raw[0] = 5;
      w->args_raw[1] = cm_len;
      w->args_raw[2] = w->key_len;
      memcpy(w->args_raw + 3, w->key, w->key_len);
      uint64_t cm_arg_objno = htobe64(w->obj_no);
      memcpy(w->args_raw + 3 + w->key_len, &cm_arg_objno, 8);
      worker_want_write(w, w->args_raw, 2 + cm_len, w->fd_manager);
      w->state = WS_COMMIT_OBJECT_READ;
      worker_want_read(w, 1, w->fd_manager);
      break;

    case WS_COMMIT_OBJECT_READ:
      worker_detect_error(w, "commit_object");
      w->state = WS_INIT;
      break;

    default:
      fprintf(stderr, "Invalid worker state: %d\n", w->state);
      exit(1);
    }
  }
}

typedef struct {
  int epoll_fd;
  _Atomic(uint64_t) workers_active;
} thread_t;

void* thread(void* thread_raw) {
  thread_t* t = (thread_t*) thread_raw;

  while (atomic_load_explicit(&t->workers_active, memory_order_relaxed)) {
    struct epoll_event events[EPOLL_MAX_EVENTS];
    // Set a timeout in case we've ended.
    int nfds = epoll_wait(t->epoll_fd, events, EPOLL_MAX_EVENTS, 100);
    if (-1 == nfds) {
      perror("Failed to epoll_wait");
      exit(1);
    }
    for (int i = 0; i < nfds; i++) {
      struct epoll_event* ev = events + i;
      worker_t* w = (worker_t*) ev->data.ptr;
      worker_yield_t y = worker(w);
      if (y.epollctl_events) {
        add_to_epoll(t->epoll_fd, y.epollctl_fd, w, y.epollctl_events, EPOLL_CTL_MOD);
      } else {
        atomic_fetch_sub_explicit(&t->workers_active, 1, memory_order_relaxed);
        close(w->fd_manager);
        close(w->fd_worker);
      }
    }
  }

  return NULL;
}

int main(void) {
  errno = 0;
  uint64_t concurrency = strtoull(getenv("CONCURRENCY"), NULL, 10);
  if (errno != 0) {
    perror("Failed to parse CONCURRENCY");
    exit(1);
  }

  errno = 0;
  uint64_t thread_count = strtoull(getenv("THREADS"), NULL, 10);
  if (errno != 0) {
    perror("Failed to parse THREADS");
    exit(1);
  }

  errno = 0;
  uint64_t count = strtoull(getenv("COUNT"), NULL, 10);
  if (errno != 0) {
    perror("Failed to parse COUNT");
    exit(1);
  }

  errno = 0;
  uint64_t size = strtoull(getenv("SIZE"), NULL, 10);
  if (errno != 0) {
    perror("Failed to parse SIZE");
    exit(1);
  }
  if (size < 8llu) {
    fprintf(stderr, "SIZE is too small\n");
    exit(1);
  }
  if (size >= 9223372036854775808llu) {
    fprintf(stderr, "SIZE is too large\n");
    exit(1);
  }

  uint64_t data_buf_size = 1llu << (64 - _lzcnt_u64(size));
  uint8_t* data = malloc(data_buf_size);
  memcpy(data, "DEADBEEF", 8);
  for (uint64_t len = 8; len < data_buf_size; len *= 2) {
    memcpy(data + len, data, len);
  }

  int epoll_fd = epoll_create1(0);
  if (-1 == epoll_fd) {
    perror("Failed to create epoll");
    exit(1);
  }

  _Atomic(uint64_t) count_started;
  atomic_init(&count_started, 0);

  worker_t* workers = malloc(sizeof(worker_t) * concurrency);
  for (uint64_t i = 0; i < concurrency; i++) {
    worker_t* worker = workers + i;
    worker->state = WS_INIT;
    worker->obj_no = 0;
    worker->key = malloc(129);
    worker->key_len = 0;
    worker->fd_manager = create_connection(SOCKET_PATH_MANAGER);
    worker->fd_worker = create_connection(SOCKET_PATH_WORKER);
    worker->data = data;
    worker->size = size;
    worker->count_started = &count_started;
    worker->count_total = count;
    worker->cur_write_data = NULL;
    worker->cur_write_completed = 0;
    worker->cur_write_total = 0;
    worker->cur_write_fd = -1;
    worker->cur_read_completed = 0;
    worker->cur_read_total = 0;
    worker->cur_read_fd = -1;

    add_to_epoll(epoll_fd, worker->fd_manager, worker, EPOLLOUT, EPOLL_CTL_ADD);
    // Ensure FD is added so that when worker returns, we can safely run EPOLL_CTL_MOD.
    add_to_epoll(epoll_fd, worker->fd_worker, worker, 0, EPOLL_CTL_ADD);
  }

  struct timespec started;
  if (-1 == clock_gettime(CLOCK_MONOTONIC, &started)) {
    perror("Failed to get current time");
    exit(1);
  }

  thread_t* thread_state = malloc(sizeof(thread_t));
  thread_state->epoll_fd = epoll_fd;
  atomic_init(&thread_state->workers_active, concurrency);

  pthread_t* threads = malloc(sizeof(pthread_t) * thread_count);
  for (uint64_t i = 0; i < thread_count; i++) {
    if (pthread_create(threads + i, NULL, thread, thread_state)) {
      perror("Failed to create thread");
      exit(1);
    }
  }
  for (uint64_t i = 0; i < thread_count; i++) {
    if (pthread_join(threads[i], NULL)) {
      perror("Failed to join thread");
      exit(1);
    }
  }

  struct timespec ended;
  if (-1 == clock_gettime(CLOCK_MONOTONIC, &ended)) {
    perror("Failed to get current time");
    exit(1);
  }

  double dur_sec = ((double) (ended.tv_sec - started.tv_sec)) + ((double) (ended.tv_nsec - started.tv_nsec)) / 1000000000.0;
  printf("Effective time: %f seconds\n", dur_sec);
  printf("Effective processing rate: %f per second\n", count / dur_sec);
  printf("Effective bandwidth: %f MiB/s\n", (count * size) / 1024.0 / 1024.0 / dur_sec);
}
