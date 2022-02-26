#include <stdbool.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "exit.h"
#include "list.h"
#include "server.h"
#include "method/create_object.h"
#include "../ext/klib/khash.h"

#define SVR_SOCK_PATH "/tmp/turbostore.sock"
#define SVR_LISTEN_BACKLOG 16384
#define SVR_EPOLL_EVENTS_MAX 128
#define SVR_CLIENTS_ACTIVE_MAX 1048576

KHASH_MAP_INIT_INT(svr_fd_to_client, uint32_t);

// `n` must be nonzero.
// Returns -1 on error or close, 0 on not ready, and nonzero on (partial) read.
static inline read_t maybe_read(int fd, uint8_t* out_buf, size_t n) {
  read_t res;
  int readno = read(fd, out_buf, n);
  if (readno == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    return -1;
  }
  return readno;
}

typedef struct {
  uint16_t read_next;
  uint16_t write_next;
  uint8_t raw_len;
  uint8_t raw[];
} svr_method_args_parser_t;

svr_method_args_parser_t* args_parser_create(size_t raw_len) {
  svr_method_args_parser_t* p = malloc(sizeof(svr_method_args_parser_t) + raw_len);
  p->read_next = 0;
  p->write_next = 0;
  p->raw_len = raw_len;
  return p;
}

void args_parser_destroy(svr_method_args_parser_t* p) {
  free(p);
}

typedef enum {
  AWAITING_CLIENT_IO,
  AWAITING_FLUSH,
  READY,
} svr_client_state_t;

typedef enum {
  // Dummy value.
  SVR_METHOD__UNKNOWN = 0,
  SVR_METHOD_CREATE_OBJECT = 1,
  // (u8 key_len, char[] key, i64 start, i64 end_exclusive_or_zero_for_eof).
  SVR_METHOD_READ_OBJECT = 2,
  // (u8 key_len, char[] key, i64 start, i64 end_exclusive).
  SVR_METHOD_WRITE_OBJECT = 3,
  // (u8 key_len, char[] key).
  SVR_METHOD_DELETE_OBJECT = 4,
} svr_method_t;

// The server is like RPC. There are methods, with one signature each. The arguments are serialised into a binary format and stored sequentially, like an ABI calling convention. Then the handler will read and write directly between the memory and socket (no user space buffering). The handler must do as much work as possible, then yield back ASAP for cooperative multitasking on one thread. This could be when writing to socket or flushing to disk. Even if we use multiple threads, it's still not ideal to waste threads by blocking on I/O tasks and idling CPU.
// The client must pass the length of the serialised arguments list in bytes. The server will only parse them once all bytes have been received.
// The server will use epoll to read and write to sockets, and then call handlers to resume handling after bytes are available/have been written.
// There is no multiplexing. A client must only send new calls after old calls have been responded. If there is an error, the client is simply disconnected.
typedef struct {
  atomic_uint_least8_t open;
  atomic_uint_least8_t state;
  int fd;
  svr_method_args_parser_t* args_parser;
  svr_method_t method;
  void* method_state;
  void (*method_state_destructor)(void*);
} svr_client_t;

typedef struct {
  svr_clients_t* svr;
  int epoll_fd;
  svr_method_handler_ctx_t* ctx;
} worker_state_t;

#define READ_OR_RELEASE(readres, fd, buf, n) \
  int readres = maybe_read(client->fd, buf, 1); \
  if (!readres) { \
    res = SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE; \
    break; \
  } \
  if (readres < 0) { \
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
    break; \
  }

void* worker_start(void* state_raw) {
  worker_state_t* state = (worker_state_t*) state_raw;
  size_t ring_buf_mask = (1 << state->svr->ring_buf_cap_log2) - 1;

  while (true) {
    if (pthread_mutex_lock(&state->svr->ready_lock)) {
      perror("Failed to lock ready clients");
      exit(EXIT_INTERNAL);
    }
    size_t read_idx = state->svr->ready_ring_buf_read;
    uint32_t client_id = state->svr->ready_ring_buf[read_idx];
    if (client_id != 0xFFFFFFFF) {
      // TODO Maybe assert `state == READY`?
      state->svr->ready_ring_buf_read = (read_idx + 1) & ring_buf_mask;
      // Take the client out of the list so others don't try to take it.
      state->svr->ready_ring_buf[read_idx] = 0xFFFFFFFF;
    }
    // Unlock ASAP.
    if (pthread_mutex_unlock(&state->svr->ready_lock)) {
      perror("Failed to unlock ready clients");
      exit(EXIT_INTERNAL);
    }
    if (client_id != 0xFFFFFFFF) {
      // We now own this client. We keep working on it and transitioning states until we get to a state where we can no longer do work immediately.
      svr_client_t* client = state->svr->ring_buf + client_id;
      svr_client_result_t res = SVR_CLIENT_RESULT__UNKNOWN;
      loop: while (true) {
        if (client->method == SVR_METHOD__UNKNOWN) {
          // We haven't parsed the method yet.
          uint8_t buf[1];
          int readlen;
          READ_OR_RELEASE(readlen, client->fd, buf, 1);
          // TODO Validate.
          client->method = buf[0];
        } else if (client->method_state == NULL) {
          // NOTE: args_parser may be NULL if we've freed it after parsing and creating method_state.
          if (client->args_parser == NULL) {
            // We haven't got the args length.
            uint8_t buf[1];
            int readlen;
            READ_OR_RELEASE(readlen, client->fd, buf, 1);
            client->args_parser = args_parser_create(buf[0]);
          } else {
            svr_method_args_parser_t* ap = client->args_parser;
            if (ap->write_next < ap->raw_len) {
              // We haven't received all args.
              int readlen;
              READ_OR_RELEASE(readlen, client->fd, &ap->raw[ap->write_next], ap->raw_len - ap->write_next);
              ap->write_next += readlen;
            } else {
              // We haven't parsed the args.
              if (method == SVR_METHOD_CREATE_OBJECT) {
                client->method_state = method_create_object_state_create(state->ctx, ap);
                client->method_state_destructor = method_create_object_state_destroy;
              } else if (method == SVR_METHOD_READ_OBJECT) {
                // TODO
              } else if (method == SVR_METHOD_WRITE_OBJECT) {
                // TODO
              } else if (method == SVR_METHOD_DELETE_OBJECT) {
                // TODO
              } else {
                fprintf(stderr, "Unknown client method %u\n", client->method);
                exit(EXIT_INTERNAL);
              }
              args_parser_destroy(client->args_parser);
              client->args_parser = NULL;
            }
          }
        } else {
          if (method == SVR_METHOD_CREATE_OBJECT) {
            res = method_create_object(state->ctx, client->method_state);
          } else if (method == SVR_METHOD_READ_OBJECT) {
            // TODO
          } else if (method == SVR_METHOD_WRITE_OBJECT) {
            // TODO
          } else if (method == SVR_METHOD_DELETE_OBJECT) {
            // TODO
          } else {
            fprintf(stderr, "Unknown client method %u\n", client->method);
            exit(EXIT_INTERNAL);
          }
          break;
        }
      }

      if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE || res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
        // Release before adding back to epoll.
        client->state = AWAITING_CLIENT_IO;
        struct epoll_event ev;
        ev.events = EPOLLONESHOT | (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE ? EPOLLIN : EPOLLOUT);
        ev.data.fd = client->fd;
        if (-1 == epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
          perror("Failed to add connection to epoll");
          exit(EXIT_INTERNAL);
        }
      } else if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
        // Release before appending to list.
        client->state = AWAITING_FLUSH;
        if (pthread_mutex_lock(&state->svr->awaiting_flush_lock)) {
          perror("Failed to acquire lock on awaiting flush list");
          exit(EXIT_INTERNAL);
        }
        client_ids_append(state->svr->awaiting_flush, client_id);
        if (pthread_mutex_unlock(&state->svr->awaiting_flush_lock)) {
          perror("Failed to acquire unlock on awaiting flush list");
          exit(EXIT_INTERNAL);
        }
      } else if (res == SVR_CLIENT_RESULT_END) {
        res = SVR_CLIENT_RESULT__UNKNOWN;
        client->state = READY;
        client->method = SVR_METHOD__UNKNOWN;
        if (client->args_parser != NULL) {
          args_parser_destroy(client->args_parser);
          client->args_parser = NULL;
        }
        if (client->method_state != NULL) {
          client->method_state_destructor(client->method_state);
          client->method_state = NULL;
          client->method_state_destructor = NULL;
        }
        goto loop;
      } else if (res == SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR) {
        if (pthread_rwlock_wrlock(&state->svr->fd_to_client_lock)) {
          perror("Failed to acquire write lock on FD map");
          exit(EXIT_INTERNAL);
        }
        // Close before removing from map as otherwise an epoll event might still emit,
        // but acquire lock before closing in case FD is reused immediately.
        if (-1 == close(client->fd)) {
          perror("Failed to close client FD");
          exit(EXIT_INTERNAL);
        }
        khint_t k = kh_get_svr_fd_to_client(state->svr->fd_to_client, client->fd);
        if (k == kh_end(state->svr->fd_to_client)) {
          fprintf(stderr, "Client does not exist\n");
          exit(EXIT_INTERNAL);
        }
        kh_del_svr_fd_to_client(state->svr->fd_to_client, k);
        if (pthread_rwlock_unlock(&state->svr->fd_to_client_lock)) {
          perror("Failed to release write lock on FD map");
          exit(EXIT_INTERNAL);
        }
        // Destroy the client.
        if (client->args_parser != NULL) {
          args_parser_destroy(client->args_parser);
        }
        if (client->method_state != NULL) {
          client->method_state_destructor(client->method_state);
        }
        client->open = false;
      } else {
        fprintf(stderr, "Unknown client action result\n");
        exit(EXIT_INTERNAL);
      }
    }
  }

  return NULL;
}

void server_start_loop(
  svr_clients_t* svr,
  size_t worker_count,
  device_t* dev,
  flush_state_t* flush,
  freelist_t* fl,
  buckets_t* bkts
) {
  int svr_socket = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (-1 == svr_socket) {
    perror("Failed to open socket");
    exit(EXIT_INTERNAL);
  }

  struct sockaddr_un svr_addr;
  svr_addr.sun_family = AF_UNIX;
  strncpy(svr_addr.sun_path, SVR_SOCK_PATH, sizeof(svr_addr.sun_path) - 1);
  if (-1 == unlink(SVR_SOCK_PATH) && errno != ENOENT) {
    perror("Failed to unlink socket");
    exit(EXIT_CONF);
  }
  if (-1 == bind(svr_socket, (struct sockaddr*) &svr_addr, sizeof(svr_addr))) {
    perror("Failed to bind socket");
    exit(EXIT_CONF);
  }
  if (-1 == chmod(SVR_SOCK_PATH, 0777)) {
    perror("Failed to chmod socket");
    exit(EXIT_CONF);
  }

  if (-1 == listen(svr_socket, SVR_LISTEN_BACKLOG)) {
    perror("Failed to listen on socket");
    exit(EXIT_INTERNAL);
  }
  printf("Now listening\n");

  int svr_epoll_fd = epoll_create1(0);
  if (-1 == svr_epoll_fd) {
    perror("Failed to create epoll");
    exit(EXIT_INTERNAL);
  }

  struct epoll_event svr_epoll_ctl_event;
  svr_epoll_ctl_event.events = EPOLLIN;
  svr_epoll_ctl_event.data.fd = svr_socket;
  if (-1 == epoll_ctl(svr_epoll_fd, EPOLL_CTL_ADD, svr_socket, &svr_epoll_ctl_event)) {
    perror("Failed to add socket to epoll");
    exit(EXIT_INTERNAL);
  }

  struct epoll_event svr_epoll_events[SVR_EPOLL_EVENTS_MAX];
  while (true) {
    int nfds = epoll_wait(svr_epoll_fd, svr_epoll_events, SVR_EPOLL_EVENTS_MAX, -1);
    if (-1 == nfds) {
      perror("Failed to wait for epoll events");
      exit(EXIT_INTERNAL);
    }
    size_t ring_buf_mask = (1 << svr->ring_buf_cap_log2) - 1;
    for (int n = 0; n < nfds; n++) {
      if (svr_epoll_events[n].data.fd == svr_socket) {
        // Server has received new socket.
        // TODO Check event type; might not be EPOLLIN.
        struct sockaddr_un peer_addr;
        socklen_t peer_addr_size = sizeof(peer_addr);
        int peer = accept4(svr_socket, (struct sockaddr*) &peer_addr, &peer_addr_size, SOCK_NONBLOCK);
        if (-1 == peer) {
          perror("Failed to accept client");
          exit(EXIT_INTERNAL);
        }

        // Add to epoll.
        svr_epoll_ctl_event.events = EPOLLIN | EPOLLONESHOT;
        svr_epoll_ctl_event.data.fd = peer;
        if (-1 == epoll_ctl(svr_epoll_fd, EPOLL_CTL_ADD, peer, &svr_epoll_ctl_event)) {
          perror("Failed to add connection to epoll");
          exit(EXIT_INTERNAL);
        }

        // Get client ID.
        if (svr->ring_buf[svr->ring_buf_write]->open) {
          // TODO Drop client instead.
          perror("Too many clients");
          exit(EXIT_CONF);
        }
        uint32_t client_id = svr->ring_buf_write;
        svr->ring_buf_write = (svr->ring_buf_write + 1) & ring_buf_mask;

        // Set client.
        svr_client_t* client = svr->ring_buf + client_id;
        client->open = true;
        client->state = READY;
        client->fd = peer;
        client->args_parser = NULL;
        client->method = SVR_METHOD__UNKNOWN;
        client->method_state = NULL;
        client->method_state_destructor = NULL;

        // Map FD to client ID.
        if (pthread_rwlock_wrlock(&svr->fd_to_client_lock)) {
          perror("Failed to acquire write lock on FD map");
          exit(EXIT_INTERNAL);
        }
        int kh_res;
        khint_t kh_it = kh_put_svr_fd_to_client(svr->fd_to_client, peer, &kh_res);
        if (-1 == kh_res) {
          fprintf(stderr, "Failed to insert client into map\n");
          exit(EXIT_INTERNAL);
        }
        kh_val(svr->fd_to_client, kh_it) = client_id;
        if (pthread_rwlock_unlock(&svr->fd_to_client_lock)) {
          perror("Failed to release write lock on FD map");
          exit(EXIT_INTERNAL);
        }
      } else {
        // A client has new I/O event.
        // TODO Check event type.
        int peer = svr_epoll_events[n].data.fd;
        if (pthread_rwlock_rdlock(&svr->fd_to_client_lock)) {
          perror("Failed to acquire read lock on FD map");
          exit(EXIT_INTERNAL);
        }
        khint_t k = kh_get_svr_fd_to_client(svr->fd_to_client, peer);
        if (k == kh_end(svr->fd_to_client)) {
          fprintf(stderr, "Client does not exist\n");
          exit(EXIT_INTERNAL);
        }
        uint32_t client_id = kh_val(svr->fd_to_client, k);
        if (pthread_rwlock_unlock(&svr->fd_to_client_lock)) {
          perror("Failed to release read lock on FD map");
          exit(EXIT_INTERNAL);
        }
        svr_client_t* client = svr->ring_buf + client_id;
        if (client->state != AWAITING_CLIENT_IO) {
          fprintf(stderr, "epoll emitted event on non-owned client\n");
          exit(EXIT_INTERNAL);
        }
        if (pthread_mutex_lock(&svr->ready_lock)) {
          perror("Failed to lock ready clients");
          exit(EXIT_INTERNAL);
        }
        if (svr->ready_ring_buf[svr->ready_ring_buf_write] != 0xFFFFFFFF) {
          fprintf(stderr, "Too many clients\n");
          exit(EXIT_INTERNAL);
        }
        // This will hand off ownership of the client.
        client->state = READY;
        svr->ready_ring_buf[svr->ready_ring_buf_write] = client_id;
        svr->ready_ring_buf_write = (svr->ready_ring_buf_write + 1) & ring_buf_mask;
        if (pthread_mutex_unlock(&svr->ready_lock)) {
          perror("Failed to unlock ready clients");
          exit(EXIT_INTERNAL);
        }
      }
    }
  }
}
