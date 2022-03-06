#define _GNU_SOURCE

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include "../ext/klib/khash.h"
#include "errno.h"
#include "exit.h"
#include "flush.h"
#include "log.h"
#include "manager.h"
#include "method/_common.h"
#include "method/commit_object.h"
#include "method/create_object.h"
#include "method/delete_object.h"
#include "method/inspect_object.h"
#include "method/read_object.h"
#include "method/write_object.h"
#include "server_client.h"
#include "server_method_args.h"
#include "server.h"
#include "util.h"

#define SVR_SOCK_PATH "/tmp/turbostore.sock"
#define SVR_LISTEN_BACKLOG 65536
#define SVR_EPOLL_EVENTS_MAX 1024
#define SVR_CLIENTS_ACTIVE_MAX 1048576

LOGGER("server");

KHASH_MAP_INIT_INT(svr_fd_to_client, svr_client_t*);

struct server_s {
  kh_svr_fd_to_client_t* fd_to_client;
  pthread_rwlock_t fd_to_client_lock;
  int svr_epoll_fd;
  int svr_socket_fd;
  manager_state_t* manager_state;
  svr_method_handler_ctx_t* ctx;
};

#define READ_OR_RELEASE(readres, fd, buf, n) \
  readres = maybe_read(fd, buf, n); \
  if (!readres) { \
    return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE; \
  } \
  if (readres < 0) { \
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
  }

static inline svr_client_result_t process_client_until_result(
  server_t* server,
  svr_client_t* client
) {
  while (true) {
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
        client->args_parser = server_method_args_parser_create(buf[0]);
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
            client->method_state = method_create_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_create_object_state_destroy;
          } else if (client->method == SVR_METHOD_INSPECT_OBJECT) {
            client->method_state = method_inspect_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_inspect_object_state_destroy;
          } else if (client->method == SVR_METHOD_READ_OBJECT) {
            client->method_state = method_read_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_read_object_state_destroy;
          } else if (client->method == SVR_METHOD_WRITE_OBJECT) {
            client->method_state = method_write_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_write_object_state_destroy;
          } else if (client->method == SVR_METHOD_COMMIT_OBJECT) {
            client->method_state = method_commit_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_commit_object_state_destroy;
          } else if (client->method == SVR_METHOD_DELETE_OBJECT) {
            client->method_state = method_delete_object_state_create(server->ctx, ap);
            client->method_state_destructor = method_delete_object_state_destroy;
          } else {
            fprintf(stderr, "Unknown client method %u\n", client->method);
            exit(EXIT_INTERNAL);
          }
          server_method_args_parser_destroy(client->args_parser);
          client->args_parser = NULL;
        }
      }
    } else {
       if (client->method == SVR_METHOD_INSPECT_OBJECT) {
        return method_inspect_object(server->ctx, client->method_state, client->fd);
      }
      if (client->method == SVR_METHOD_READ_OBJECT) {
        return method_read_object(server->ctx, client->method_state, client->fd);
      }
      if (client->method == SVR_METHOD_WRITE_OBJECT) {
        return method_write_object(server->ctx, client->method_state, client->fd);
      }
      if (client->method == SVR_METHOD_COMMIT_OBJECT || client->method == SVR_METHOD_CREATE_OBJECT || client->method == SVR_METHOD_DELETE_OBJECT) {
        if (!client->already_processed_by_manager) {
          client->already_processed_by_manager = true;
          svr_client_t* popped = manager_hand_off_client(server->manager_state, client);
          if (popped) {
            // We are too overloaded, drop the client.
            server_hand_back_client_from_manager(server, popped, SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR);
          }
          return SVR_CLIENT_RESULT_HANDED_OFF_TO_MANAGER;
        }
        if (client->method == SVR_METHOD_CREATE_OBJECT) {
          return method_create_object(server->ctx, client->method_state, client->fd);
        }
        if (client->method == SVR_METHOD_COMMIT_OBJECT) {
          return method_commit_object(server->ctx, client->method_state, client->fd);
        }
        if (client->method == SVR_METHOD_DELETE_OBJECT) {
          return method_delete_object(server->ctx, client->method_state, client->fd);
        }
      }
      fprintf(stderr, "Unknown client method %u\n", client->method);
      exit(EXIT_INTERNAL);
    }
  }
}

static inline void worker_handle_client_ready(
  server_t* server,
  svr_client_t* client
) {
  while (true) {
    svr_client_result_t res = client->method_result_from_manager;
    if (res == SVR_CLIENT_RESULT__UNKNOWN) {
      res = process_client_until_result(server, client);
    } else {
      client->method_result_from_manager = SVR_CLIENT_RESULT__UNKNOWN;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE || res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
      struct epoll_event ev;
      ev.events = EPOLLET | EPOLLONESHOT | (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE ? EPOLLIN : EPOLLOUT);
      ev.data.fd = client->fd;
      if (-1 == epoll_ctl(server->svr_epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
        perror("Failed to add connection to epoll");
        exit(EXIT_INTERNAL);
      }
      break;
    }

    if (res == SVR_CLIENT_RESULT_HANDED_OFF_TO_MANAGER) {
      break;
    }

    if (res == SVR_CLIENT_RESULT_END) {
      res = SVR_CLIENT_RESULT__UNKNOWN;
      client->method = SVR_METHOD__UNKNOWN;
      if (client->args_parser != NULL) {
        server_method_args_parser_destroy(client->args_parser);
        client->args_parser = NULL;
      }
      if (client->method_state != NULL) {
        client->method_state_destructor(client->method_state);
        client->method_state = NULL;
        client->method_state_destructor = NULL;
      }
      client->already_processed_by_manager = false;
      continue;
    }

    if (res == SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR) {
      ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&server->fd_to_client_lock), "acquire write lock on FD map");
      // Close before removing from map as otherwise an epoll event might still emit,
      // but acquire lock before closing in case FD is reused immediately.
      if (-1 == close(client->fd)) {
        perror("Failed to close client FD");
        exit(EXIT_INTERNAL);
      }
      khint_t k = kh_get_svr_fd_to_client(server->fd_to_client, client->fd);
      if (k == kh_end(server->fd_to_client)) {
        fprintf(stderr, "Client does not exist\n");
        exit(EXIT_INTERNAL);
      }
      kh_del_svr_fd_to_client(server->fd_to_client, k);
      ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&server->fd_to_client_lock), "release write lock on FD map");
      // Destroy the client.
      if (client->args_parser != NULL) {
        server_method_args_parser_destroy(client->args_parser);
      }
      if (client->method_state != NULL) {
        client->method_state_destructor(client->method_state);
      }
      free(client);
      break;
    }

    fprintf(stderr, "Unknown client (method=%d) action result: %d\n", client->method, res);
    exit(EXIT_INTERNAL);
  }
}

void* worker_start(void* server_raw) {
  server_t* server = (server_t*) server_raw;

  struct epoll_event svr_epoll_events[SVR_EPOLL_EVENTS_MAX];
  while (true) {
    int nfds = epoll_wait(server->svr_epoll_fd, svr_epoll_events, SVR_EPOLL_EVENTS_MAX, -1);
    if (-1 == nfds) {
      perror("Failed to wait for epoll events");
      exit(EXIT_INTERNAL);
    }
    for (int n = 0; n < nfds; n++) {
      if (svr_epoll_events[n].data.fd == server->svr_socket_fd) {
        // Server has received new socket.
        // TODO Check event type; might not be EPOLLIN.
        struct sockaddr_un peer_addr;
        socklen_t peer_addr_size = sizeof(peer_addr);
        int peer = accept4(server->svr_socket_fd, (struct sockaddr*) &peer_addr, &peer_addr_size, SOCK_NONBLOCK);
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
        if (-1 == epoll_ctl(server->svr_epoll_fd, EPOLL_CTL_ADD, peer, &ev)) {
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
        client->method_result_from_manager = SVR_CLIENT_RESULT__UNKNOWN;
        client->already_processed_by_manager = false;

        // Map FD to client ID.
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_wrlock(&server->fd_to_client_lock), "acquire write lock on FD map");
        int kh_res;
        khint_t kh_it = kh_put_svr_fd_to_client(server->fd_to_client, peer, &kh_res);
        if (-1 == kh_res) {
          fprintf(stderr, "Failed to insert client into map\n");
          exit(EXIT_INTERNAL);
        }
        kh_val(server->fd_to_client, kh_it) = client;
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&server->fd_to_client_lock), "release write lock on FD map");
      } else {
        // A client has new I/O event.
        int peer = svr_epoll_events[n].data.fd;
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_rdlock(&server->fd_to_client_lock), "acquire read lock on FD map");
        khint_t k = kh_get_svr_fd_to_client(server->fd_to_client, peer);
        if (k == kh_end(server->fd_to_client)) {
          fprintf(stderr, "Client does not exist\n");
          exit(EXIT_INTERNAL);
        }
        svr_client_t* client = kh_val(server->fd_to_client, k);
        ASSERT_ERROR_RETVAL_OK(pthread_rwlock_unlock(&server->fd_to_client_lock), "release read lock on FD map");
        worker_handle_client_ready(server, client);
      }
    }
  }

  return NULL;
}

server_t* server_create(
  device_t* dev,
  freelist_t* fl,
  inodes_state_t* inodes_state,
  buckets_t* bkts,
  stream_t* stream,
  flush_state_t* flush_state,
  manager_state_t* manager_state
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
  svr->manager_state = manager_state;
  svr->fd_to_client = kh_init_svr_fd_to_client();
  ASSERT_ERROR_RETVAL_OK(pthread_rwlock_init(&svr->fd_to_client_lock, NULL), "create lock for FD map");
  svr->svr_epoll_fd = svr_epoll_fd;
  svr->svr_socket_fd = svr_socket;
  svr->ctx = malloc(sizeof(svr_method_handler_ctx_t));
  svr->ctx->inodes_state = inodes_state;
  svr->ctx->bkts = bkts;
  svr->ctx->dev = dev;
  svr->ctx->fl = fl;
  svr->ctx->flush_state = flush_state;
  svr->ctx->stream = stream;
  return svr;
}

svr_method_handler_ctx_t* server_get_method_handler_context(server_t* server) {
  return server->ctx;
}

void server_hand_back_client_from_manager(server_t* clients, svr_client_t* client, svr_client_result_t result) {
  client->method_result_from_manager = result;
  struct epoll_event ev;
  ev.events = EPOLLET | EPOLLONESHOT | (result == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE ? EPOLLIN : 0) | (result == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE ? EPOLLOUT : 0);
  ev.data.fd = client->fd;
  if (-1 == epoll_ctl(clients->svr_epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
    perror("Failed to add connection to epoll");
    exit(EXIT_INTERNAL);
  }
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
