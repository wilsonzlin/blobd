#define _GNU_SOURCE

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include "exit.h"
#include "log.h"
#include "method/_common.h"
#include "server.h"
#include "server_client.h"

#define SERVER_EPOLL_EVENTS_MAX 1024
#define SERVER_LISTEN_BACKLOG 65536

LOGGER("server");

struct server_methods_s {
  server_method_state_creator* state_creators[256];
  server_method_handler* handlers[256];
  server_method_state_destructor* destructors[256];
};

server_methods_t* server_methods_create() {
  // NULL is not guaranteed to be 0, so cannot simply use calloc.
  server_methods_t* methods = malloc(sizeof(server_methods_t));
  for (int i = 0; i < 256; i++) {
    methods->state_creators[i] = NULL;
    methods->handlers[i] = NULL;
    methods->destructors[i] = NULL;
  }
  return methods;
}

void server_methods_add(server_methods_t* methods, method_t method, server_method_state_creator* state_creator, server_method_handler* handler, server_method_state_destructor* destructor) {
  methods->state_creators[method] = state_creator;
  methods->handlers[method] = handler;
  methods->destructors[method] = destructor;
}

struct server_s {
  int socket_fd;
  int epoll_fd;
  void* callback_state;
  server_on_client_add_handler* on_client_add;
  server_on_client_event_handler* on_client_event;
  void* method_ctx;
  server_methods_t* methods;
};

server_t* server_create(
  char* unix_socket_path,
  void* callback_state,
  server_on_client_add_handler* on_client_add,
  server_on_client_event_handler* on_client_event,
  void* method_ctx,
  server_methods_t* methods
) {
  int svr_socket = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (-1 == svr_socket) {
    perror("Failed to open socket");
    exit(EXIT_INTERNAL);
  }

  struct sockaddr_un svr_addr;
  svr_addr.sun_family = AF_UNIX;
  strncpy(svr_addr.sun_path, unix_socket_path, sizeof(svr_addr.sun_path) - 1);
  if (-1 == unlink(unix_socket_path) && errno != ENOENT) {
    perror("Failed to unlink socket");
    exit(EXIT_CONF);
  }
  if (-1 == bind(svr_socket, (struct sockaddr*) &svr_addr, sizeof(svr_addr))) {
    perror("Failed to bind socket");
    exit(EXIT_CONF);
  }
  if (-1 == chmod(unix_socket_path, 0777)) {
    perror("Failed to chmod socket");
    exit(EXIT_CONF);
  }

  if (-1 == listen(svr_socket, SERVER_LISTEN_BACKLOG)) {
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
  svr->socket_fd = svr_socket;
  svr->epoll_fd = svr_epoll_fd;
  svr->callback_state = callback_state;
  svr->on_client_add = on_client_add;
  svr->on_client_event = on_client_event;
  svr->method_ctx = method_ctx;
  svr->methods = methods;
  return svr;
}

void server_wait_epoll(server_t* server, int timeout) {
  struct epoll_event svr_epoll_events[SERVER_EPOLL_EVENTS_MAX];
  int nfds = epoll_wait(server->epoll_fd, svr_epoll_events, SERVER_EPOLL_EVENTS_MAX, timeout);
  if (-1 == nfds) {
    perror("Failed to wait for epoll events");
    exit(EXIT_INTERNAL);
  }
  for (int n = 0; n < nfds; n++) {
    if (svr_epoll_events[n].data.fd == server->socket_fd) {
      // Server has received new socket.
      // TODO Check event type; might not be EPOLLIN.
      struct sockaddr_un peer_addr;
      socklen_t peer_addr_size = sizeof(peer_addr);
      int peer = accept4(server->socket_fd, (struct sockaddr*) &peer_addr, &peer_addr_size, SOCK_NONBLOCK);
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
      if (-1 == epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, peer, &ev)) {
        perror("Failed to add connection to epoll");
        exit(EXIT_INTERNAL);
      }

      server->on_client_add(server->callback_state, peer);
    } else {
      server->on_client_event(server->callback_state, svr_epoll_events[n].data.fd);
    }
  }
}

void server_rearm_client_to_epoll(server_t* server, int client_fd, bool read, bool write) {
  struct epoll_event ev;
  ev.events = EPOLLET | EPOLLONESHOT | (read ? EPOLLIN : 0) | (write ? EPOLLOUT : 0);
  ev.data.fd = client_fd;
  if (-1 == epoll_ctl(server->epoll_fd, EPOLL_CTL_MOD, client_fd, &ev)) {
    perror("Failed to add connection to epoll");
    exit(EXIT_INTERNAL);
  }
}

#define READ_OR_RELEASE(readres, fd, buf, n) \
  readres = maybe_read(fd, buf, n); \
  if (!readres) { \
    return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE; \
  } \
  if (readres < 0) { \
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
  }

svr_client_result_t server_process_client_until_result(server_t* server, svr_client_t* client) {
  while (true) {
    if (client->method == METHOD__UNKNOWN) {
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
          server_method_state_creator* fn = server->methods->state_creators[client->method];
          if (fn == NULL) {
            // TODO
          }
          client->method_state = fn(server->method_ctx, ap);
          client->method_state_destructor = server->methods->destructors[client->method];
          server_method_args_parser_destroy(client->args_parser);
          client->args_parser = NULL;
        }
      }
    } else {
      // The method must exist.
      server_method_handler* fn = server->methods->handlers[client->method];
      return fn(server->method_ctx, client->method_state, client->fd);
    }
  }
}
