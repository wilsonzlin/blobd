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
#include "log.h"
#include "server.h"
#include "util.h"
#include "method/commit_object.h"
#include "method/create_object.h"
#include "method/delete_object.h"
#include "method/inspect_object.h"
#include "method/read_object.h"
#include "method/write_object.h"
#include "../ext/klib/khash.h"

#define SVR_SOCK_PATH "/tmp/turbostore.sock"
#define SVR_LISTEN_BACKLOG 16384
#define SVR_EPOLL_EVENTS_MAX 128
#define SVR_CLIENTS_ACTIVE_MAX 1048576

LOGGER("server");

struct svr_method_args_parser_s {
  uint16_t read_next;
  uint16_t write_next;
  uint8_t raw_len;
  uint8_t raw[];
};

svr_method_args_parser_t* args_parser_create(uint64_t raw_len) {
  svr_method_args_parser_t* p = malloc(sizeof(svr_method_args_parser_t) + raw_len);
  p->read_next = 0;
  p->write_next = 0;
  p->raw_len = raw_len;
  return p;
}

void args_parser_destroy(svr_method_args_parser_t* p) {
  free(p);
}

uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, uint64_t want_bytes) {
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

typedef struct {
  int fd;
  svr_method_args_parser_t* args_parser;
  svr_method_t method;
  void* method_state;
  void (*method_state_destructor)(void*);
} svr_client_t;

KHASH_MAP_INIT_INT(svr_fd_to_client, svr_client_t*);

LIST_DEF(client_list, svr_client_t*);
LIST(client_list, svr_client_t*);

struct server_s {
  kh_svr_fd_to_client_t* fd_to_client;
  pthread_rwlock_t fd_to_client_lock;
  client_list_t* awaiting_flush;
  pthread_mutex_t awaiting_flush_lock;
  client_list_t* flushing;
  int svr_epoll_fd;
  int svr_socket_fd;
  svr_method_handler_ctx_t* ctx;
};

#define READ_OR_RELEASE(readres, fd, buf, n) \
  readres = maybe_read(fd, buf, n); \
  if (!readres) { \
    res = SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE; \
    break; \
  } \
  if (readres < 0) { \
    res = SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
    break; \
  }

static inline void worker_handle_client_ready(
  server_t* state,
  svr_client_t* client
) {
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
          READ_OR_RELEASE(readlen, client->fd, ap->raw + ap->write_next, ap->raw_len - ap->write_next);
          ap->write_next += readlen;
        } else {
          // We haven't parsed the args.
          if (client->method == SVR_METHOD_CREATE_OBJECT) {
            client->method_state = method_create_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_create_object_state_destroy;
          } else if (client->method == SVR_METHOD_INSPECT_OBJECT) {
            client->method_state = method_inspect_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_inspect_object_state_destroy;
          } else if (client->method == SVR_METHOD_READ_OBJECT) {
            client->method_state = method_read_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_read_object_state_destroy;
          } else if (client->method == SVR_METHOD_WRITE_OBJECT) {
            client->method_state = method_write_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_write_object_state_destroy;
          } else if (client->method == SVR_METHOD_COMMIT_OBJECT) {
            client->method_state = method_commit_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_commit_object_state_destroy;
          } else if (client->method == SVR_METHOD_DELETE_OBJECT) {
            client->method_state = method_delete_object_state_create(state->ctx, ap);
            client->method_state_destructor = method_delete_object_state_destroy;
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
      } else if (client->method == SVR_METHOD_INSPECT_OBJECT) {
        res = method_inspect_object(state->ctx, client->method_state, client->fd);
      } else if (client->method == SVR_METHOD_READ_OBJECT) {
        res = method_read_object(state->ctx, client->method_state, client->fd);
      } else if (client->method == SVR_METHOD_WRITE_OBJECT) {
        res = method_write_object(state->ctx, client->method_state, client->fd);
      } else if (client->method == SVR_METHOD_COMMIT_OBJECT) {
        res = method_commit_object(state->ctx, client->method_state, client->fd);
      } else if (client->method == SVR_METHOD_DELETE_OBJECT) {
        res = method_delete_object(state->ctx, client->method_state, client->fd);
      } else {
        fprintf(stderr, "Unknown client method %u\n", client->method);
        exit(EXIT_INTERNAL);
      }
      break;
    }
  }

  if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE || res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLONESHOT | (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE ? EPOLLIN : EPOLLOUT);
    ev.data.fd = client->fd;
    if (-1 == epoll_ctl(state->svr_epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
      perror("Failed to add connection to epoll");
      exit(EXIT_INTERNAL);
    }
  } else if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
    ASSERT_ERROR_RETVAL_OK(pthread_mutex_lock(&state->awaiting_flush_lock), "acquire lock on awaiting flush list");
    client_list_append(state->awaiting_flush, client);
    ASSERT_ERROR_RETVAL_OK(pthread_mutex_unlock(&state->awaiting_flush_lock), "acquire unlock on awaiting flush list");
  } else if (res == SVR_CLIENT_RESULT_END) {
    res = SVR_CLIENT_RESULT__UNKNOWN;
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
    ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&state->fd_to_client_lock), "acquire write lock on FD map");
    // Close before removing from map as otherwise an epoll event might still emit,
    // but acquire lock before closing in case FD is reused immediately.
    if (-1 == close(client->fd)) {
      perror("Failed to close client FD");
      exit(EXIT_INTERNAL);
    }
    khint_t k = kh_get_svr_fd_to_client(state->fd_to_client, client->fd);
    if (k == kh_end(state->fd_to_client)) {
      fprintf(stderr, "Client does not exist\n");
      exit(EXIT_INTERNAL);
    }
    kh_del_svr_fd_to_client(state->fd_to_client, k);
    ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&state->fd_to_client_lock), "release write lock on FD map");
    // Destroy the client.
    if (client->args_parser != NULL) {
      args_parser_destroy(client->args_parser);
    }
    if (client->method_state != NULL) {
      client->method_state_destructor(client->method_state);
    }
    free(client);
  } else {
    fprintf(stderr, "Unknown client action result\n");
    exit(EXIT_INTERNAL);
  }
}

void* worker_start(void* state_raw) {
  server_t* state = (server_t*) state_raw;

  struct epoll_event svr_epoll_events[SVR_EPOLL_EVENTS_MAX];
  while (true) {
    int nfds = epoll_wait(state->svr_epoll_fd, svr_epoll_events, SVR_EPOLL_EVENTS_MAX, -1);
    if (-1 == nfds) {
      perror("Failed to wait for epoll events");
      exit(EXIT_INTERNAL);
    }
    for (int n = 0; n < nfds; n++) {
      if (svr_epoll_events[n].data.fd == state->svr_socket_fd) {
        // Server has received new socket.
        // TODO Check event type; might not be EPOLLIN.
        struct sockaddr_un peer_addr;
        socklen_t peer_addr_size = sizeof(peer_addr);
        int peer = accept4(state->svr_socket_fd, (struct sockaddr*) &peer_addr, &peer_addr_size, SOCK_NONBLOCK);
        if (-1 == peer) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
          }
          perror("Failed to accept client");
          exit(EXIT_INTERNAL);
        }

        // Add to epoll.
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET;
        ev.data.fd = peer;
        if (-1 == epoll_ctl(state->svr_epoll_fd, EPOLL_CTL_ADD, peer, &ev)) {
          perror("Failed to add connection to epoll");
          exit(EXIT_INTERNAL);
        }

        // Set client.
        svr_client_t* client = malloc(sizeof(svr_client_t));
        client->fd = peer;
        client->args_parser = NULL;
        client->method = SVR_METHOD__UNKNOWN;
        client->method_state = NULL;
        client->method_state_destructor = NULL;

        // Map FD to client ID.
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&state->fd_to_client_lock), "acquire write lock on FD map");
        int kh_res;
        khint_t kh_it = kh_put_svr_fd_to_client(state->fd_to_client, peer, &kh_res);
        if (-1 == kh_res) {
          fprintf(stderr, "Failed to insert client into map\n");
          exit(EXIT_INTERNAL);
        }
        kh_val(state->fd_to_client, kh_it) = client;
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&state->fd_to_client_lock), "release write lock on FD map");
      } else {
        // A client has new I/O event.
        // TODO Check event type.
        int peer = svr_epoll_events[n].data.fd;
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&state->fd_to_client_lock), "acquire read lock on FD map");
        khint_t k = kh_get_svr_fd_to_client(state->fd_to_client, peer);
        if (k == kh_end(state->fd_to_client)) {
          fprintf(stderr, "Client does not exist\n");
          exit(EXIT_INTERNAL);
        }
        svr_client_t* client = kh_val(state->fd_to_client, k);
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&state->fd_to_client_lock), "release read lock on FD map");
        worker_handle_client_ready(state, client);
      }
    }
  }

  return NULL;
}

server_t* server_create(
  device_t* dev,
  flush_state_t* flush,
  freelist_t* fl,
  buckets_t* bkts,
  stream_t* stream
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
  DEBUG_TS_LOG("Listening");

  int svr_epoll_fd = epoll_create1(0);
  if (-1 == svr_epoll_fd) {
    perror("Failed to create epoll");
    exit(EXIT_INTERNAL);
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = svr_socket;
  if (-1 == epoll_ctl(svr_epoll_fd, EPOLL_CTL_ADD, svr_socket, &ev)) {
    perror("Failed to add socket to epoll");
    exit(EXIT_INTERNAL);
  }

  server_t* svr = malloc(sizeof(server_t));
  svr->fd_to_client = kh_init_svr_fd_to_client();
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&svr->fd_to_client_lock, NULL), "create lock for FD map");
  svr->awaiting_flush = client_list_create();
  ASSERT_ERROR_RETVAL_OK(pthread_mutex_init(&svr->awaiting_flush_lock, NULL), "create lock for awaiting flush list");
  svr->flushing = client_list_create();
  svr->svr_epoll_fd = svr_epoll_fd;
  svr->svr_socket_fd = svr_socket;
  svr->ctx = malloc(sizeof(svr_method_handler_ctx_t));
  svr->ctx->bkts = bkts;
  svr->ctx->dev = dev;
  svr->ctx->fl = fl;
  svr->ctx->flush = flush;
  svr->ctx->stream = stream;
  return svr;
}

bool server_on_flush_start(server_t* clients) {
  ASSERT_ERROR_RETVAL_OK(pthread_mutex_lock(&clients->awaiting_flush_lock), "acquire lock on awaiting flush list");
  bool has_awaiting = !!clients->awaiting_flush->len;
  if (has_awaiting) {
    if (clients->flushing->cap < clients->awaiting_flush->len) {
      while (clients->flushing->cap < clients->awaiting_flush->len) {
        clients->flushing->cap *= 2;
      }
      clients->flushing->elems = realloc(clients->flushing->elems, clients->flushing->cap * sizeof(svr_client_t*));
    }
    memcpy(clients->flushing->elems, clients->awaiting_flush->elems, clients->awaiting_flush->len * sizeof(svr_client_t*));
    clients->flushing->len = clients->awaiting_flush->len;
    clients->awaiting_flush->len = 0;
  }
  ASSERT_ERROR_RETVAL_OK(pthread_mutex_unlock(&clients->awaiting_flush_lock), "release lock on awaiting flush list");
  return has_awaiting;
}

void server_on_flush_end(server_t* clients) {
  for (uint64_t i = 0; i < clients->flushing->len; i++) {
    svr_client_t* client = clients->flushing->elems[i];
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLONESHOT | EPOLLOUT;
    ev.data.fd = client->fd;
    if (-1 == epoll_ctl(clients->svr_epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
      perror("Failed to add connection to epoll");
      exit(EXIT_INTERNAL);
    }
  }
  clients->flushing->len = 0;
}

void server_start_loop(
  server_t* svr,
  uint64_t worker_count
) {
  pthread_t* threads = malloc(sizeof(pthread_t) * worker_count);
  for (uint64_t i = 0; i < worker_count; i++) {
    ASSERT_ERROR_RETVAL_OK(pthread_create(&threads[i], NULL, worker_start, svr), "create server worker");
  }

  for (uint64_t i = 0; i < worker_count; i++) {
    ASSERT_ERROR_RETVAL_OK(pthread_join(threads[i], NULL), "join server worker");
  }

  fprintf(stderr, "Reached end of server loop\n");
  exit(EXIT_INTERNAL);
}
