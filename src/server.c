#define _GNU_SOURCE

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include "errno.h"
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

LIST_DEF(client_ids, uint32_t);
LIST(client_ids, uint32_t);

// `n` must be nonzero.
// Returns -1 on error or close, 0 on not ready, and nonzero on (partial) read.
static inline int maybe_read(int fd, uint8_t* out_buf, size_t n) {
  int readno = read(fd, out_buf, n);
  if (readno == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    return -1;
  }
  return readno;
}

struct svr_method_args_parser_s {
  uint16_t read_next;
  uint16_t write_next;
  uint8_t raw_len;
  uint8_t raw[];
};

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

uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, size_t want_bytes) {
  if (parser->read_next + want_bytes > parser->raw_len) {
    return NULL;
  }
  uint8_t* rv = parser->raw + parser->read_next;
  parser->read_next += want_bytes;
  return rv;
}

bool svr_method_args_parser_end(svr_method_args_parser_t* parser) {
  return parser->read_next == parser->raw_len;
}

struct svr_clients_s {
  kh_svr_fd_to_client_t* fd_to_client;
  pthread_rwlock_t fd_to_client_lock;
  svr_client_t* ring_buf;
  size_t ring_buf_cap_log2;
  size_t ring_buf_read;
  size_t ring_buf_write;
  // The size of this ring buffer should also be ring_buf_cap_log2.
  uint32_t* ready_ring_buf;
  size_t ready_ring_buf_read;
  size_t ready_ring_buf_write;
  pthread_mutex_t ready_lock;
  client_ids_t* awaiting_flush;
  pthread_mutex_t awaiting_flush_lock;
};

typedef struct {
  svr_clients_t* svr;
  int epoll_fd;
  svr_method_handler_ctx_t* ctx;
} worker_state_t;

#define READ_OR_RELEASE(readres, fd, buf, n) \
  readres = maybe_read(fd, buf, 1); \
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
              if (client->method == SVR_METHOD_CREATE_OBJECT) {
                client->method_state = method_create_object_state_create(state->ctx, ap);
                client->method_state_destructor = method_create_object_state_destroy;
              } else if (client->method == SVR_METHOD_READ_OBJECT) {
                // TODO
              } else if (client->method == SVR_METHOD_WRITE_OBJECT) {
                // TODO
              } else if (client->method == SVR_METHOD_DELETE_OBJECT) {
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
          if (client->method == SVR_METHOD_CREATE_OBJECT) {
            res = method_create_object(state->ctx, client->method_state, client->fd);
          } else if (client->method == SVR_METHOD_READ_OBJECT) {
            // TODO
          } else if (client->method == SVR_METHOD_WRITE_OBJECT) {
            // TODO
          } else if (client->method == SVR_METHOD_DELETE_OBJECT) {
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
        client->state = SVR_CLIENT_STATE_AWAITING_CLIENT_IO;
        struct epoll_event ev;
        ev.events = EPOLLONESHOT | (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE ? EPOLLIN : EPOLLOUT);
        ev.data.fd = client->fd;
        if (-1 == epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
          perror("Failed to add connection to epoll");
          exit(EXIT_INTERNAL);
        }
      } else if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
        // Release before appending to list.
        client->state = SVR_CLIENT_STATE_AWAITING_FLUSH;
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
        client->state = SVR_CLIENT_STATE_READY;
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

svr_clients_t* server_clients_create(size_t max_clients_log2) {
  svr_clients_t* clients = malloc(sizeof(svr_clients_t));
  clients->fd_to_client = kh_init_svr_fd_to_client();
  if (pthread_rwlock_init(&clients->fd_to_client_lock, NULL)) {
    perror("Failed to create lock for FD map");
    exit(EXIT_INTERNAL);
  }
  clients->ring_buf = malloc(sizeof(svr_client_t) * (1 << max_clients_log2));
  clients->ring_buf_cap_log2 = max_clients_log2;
  clients->ring_buf_read = 0;
  clients->ring_buf_write = 0;
  clients->ready_ring_buf = malloc(sizeof(uint32_t) * (1 << max_clients_log2));
  clients->ready_ring_buf_read = 0;
  clients->ready_ring_buf_write = 0;
  if (pthread_mutex_init(&clients->ready_lock, NULL)) {
    perror("Failed to create lock for ready clients");
    exit(EXIT_INTERNAL);
  }
  clients->awaiting_flush = client_ids_create_with_capacity(1 << max_clients_log2);
  if (pthread_mutex_init(&clients->awaiting_flush_lock, NULL)) {
    perror("Failed to create lock for awaiting flush list");
    exit(EXIT_INTERNAL);
  }
  return clients;
}

size_t server_clients_get_capacity(svr_clients_t* clients) {
  return 1 << clients->ring_buf_cap_log2;
}

void server_clients_acquire_awaiting_flush_lock(svr_clients_t* clients) {
  if (pthread_mutex_lock(&clients->awaiting_flush_lock)) {
    perror("Failed to acquire lock on awaiting flush list");
    exit(EXIT_INTERNAL);
  }
}

void server_clients_release_awaiting_flush_lock(svr_clients_t* clients) {
  if (pthread_mutex_unlock(&clients->awaiting_flush_lock)) {
    perror("Failed to release lock on awaiting flush list");
    exit(EXIT_INTERNAL);
  }
}

size_t server_clients_get_awaiting_flush_count(svr_clients_t* clients) {
  return clients->awaiting_flush->len;
}

void server_clients_pop_all_awaiting_flush(svr_clients_t* clients, uint32_t* out) {
  memcpy(out, clients->awaiting_flush->elems, clients->awaiting_flush->len);
  clients->awaiting_flush->len = 0;
}

void server_clients_push_all_ready(svr_clients_t* clients, uint32_t* client_ids, size_t n) {
  if (pthread_mutex_lock(&clients->ready_lock)) {
    perror("Failed to lock ready clients");
    exit(EXIT_INTERNAL);
  }
  for (size_t i = 0; i < n; i++) {
    uint32_t client_id = client_ids[i];
    if (clients->ready_ring_buf[clients->ready_ring_buf_write] != 0xFFFFFFFF) {
      fprintf(stderr, "Too many clients\n");
      exit(EXIT_INTERNAL);
    }
    clients->ring_buf[client_id].state = SVR_CLIENT_STATE_READY;
    clients->ready_ring_buf[clients->ready_ring_buf_write] = client_id;
    clients->ready_ring_buf_write = (clients->ready_ring_buf_write + 1) & ((1 << clients->ring_buf_cap_log2) - 1);
  }
  if (pthread_mutex_unlock(&clients->ready_lock)) {
    perror("Failed to unlock ready clients");
    exit(EXIT_INTERNAL);
  }
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

  worker_state_t* worker_state = malloc(sizeof(worker_state_t));
  worker_state->svr = svr;
  worker_state->epoll_fd = svr_epoll_fd;
  worker_state->ctx = malloc(sizeof(svr_method_handler_ctx_t));
  worker_state->ctx->bkts = bkts;
  worker_state->ctx->dev = dev;
  worker_state->ctx->fl = fl;
  worker_state->ctx->flush = flush;
  for (size_t i = 0; i < worker_count; i++) {
    pthread_t t;
    if (pthread_create(&t, NULL, worker_start, worker_state)) {
      perror("Failed to create server worker");
      exit(EXIT_INTERNAL);
    }
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
        if (svr->ring_buf[svr->ring_buf_write].open) {
          // TODO Drop client instead.
          perror("Too many clients");
          exit(EXIT_CONF);
        }
        uint32_t client_id = svr->ring_buf_write;
        svr->ring_buf_write = (svr->ring_buf_write + 1) & ring_buf_mask;

        // Set client.
        svr_client_t* client = svr->ring_buf + client_id;
        client->open = true;
        client->state = SVR_CLIENT_STATE_READY;
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
        if (client->state != SVR_CLIENT_STATE_AWAITING_CLIENT_IO) {
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
        client->state = SVR_CLIENT_STATE_READY;
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
