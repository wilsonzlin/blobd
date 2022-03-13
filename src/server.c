#include <arpa/inet.h>
#include <errno.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <stdatomic.h>
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
#include "method/commit_object.h"
#include "method/create_object.h"
#include "method/delete_object.h"
#include "method/inspect_object.h"
#include "method/read_object.h"
#include "method/write_object.h"
#include "server_client.h"
#include "server.h"

// Make sure this is big enough as we flush after each epoll_wait(), and we don't want to do too many flushes in a short timespan (inefficient use of disk I/O).
#define SERVER_EPOLL_EVENTS_MAX 8192
#define SERVER_LISTEN_BACKLOG 65536

LOGGER("server");

// Called with method_state, method_ctx and args_parser.
typedef void (server_method_state_initialiser)(void*, method_ctx_t*, svr_method_args_parser_t*);

// Called with method_ctx, method_state, and client; returns client_result.
typedef svr_client_result_t (server_method_handler)(method_ctx_t*, void*, svr_client_t*);

typedef struct {
  server_method_state_initialiser* state_initialisers[256];
  server_method_handler* handlers[256];
} server_methods_t;

struct server_s {
  server_clients_t* clients;
  int socket_fd;
  int epoll_fd;
  method_ctx_t* method_ctx;
  server_methods_t methods;
};

server_t* server_create(
  // Can be NULL.
  char* address,
  // Must be 0 if `unix_socket_path` is not NULL.
  uint16_t port,
  // Can be NULL.
  char* unix_socket_path,
  method_ctx_t* method_ctx
) {
  int skt = socket(unix_socket_path ? AF_UNIX : AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (-1 == skt) {
    perror("Failed to open socket");
    exit(EXIT_INTERNAL);
  }
  if (!unix_socket_path) {
    int opt = 1;
    // Set SO_REUSEADDR to allow immediate reuse of address/port combo if we crashed.
    if (-1 == setsockopt(skt, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int))) {
      perror("Failed to set SO_REUSEADDR");
      exit(EXIT_INTERNAL);
    }
    opt = 1;
    // Disable Nagle's algorithm.
    if (-1 == setsockopt(skt, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(int))) {
      perror("Failed to set TCP_NODELAY");
      exit(EXIT_INTERNAL);
    }
  }

  struct sockaddr* addr;
  uint64_t addr_size;
  if (unix_socket_path) {
    ts_log(INFO, "Binding to UNIX socket path %s", unix_socket_path);
    addr_size = sizeof(struct sockaddr_un);
    struct sockaddr_un* un = malloc(addr_size);
    un->sun_family = AF_UNIX;
    strncpy(un->sun_path, unix_socket_path, sizeof(un->sun_path) - 1);
    if (-1 == unlink(unix_socket_path) && errno != ENOENT) {
      perror("Failed to unlink socket");
      exit(EXIT_CONF);
    }
    addr = (struct sockaddr*) un;
  } else {
    addr_size = sizeof(struct sockaddr_in);
    struct sockaddr_in* in = malloc(addr_size);
    in->sin_family = AF_INET;
    if (!address) {
      ts_log(INFO, "Binding to TCP port %u on all addresses", port);
      in->sin_addr.s_addr = INADDR_ANY;
    } else {
      if (!inet_aton(address, &in->sin_addr)) {
        fprintf(stderr, "Invalid address: %s\n", address);
        exit(EXIT_CONF);
      }
      ts_log(INFO, "Binding to TCP port %u on address %s", port, address);
    }
    in->sin_port = htons(port);
    addr = (struct sockaddr*) in;
  }
  if (-1 == bind(skt, addr, addr_size)) {
    perror("Failed to bind socket");
    exit(EXIT_CONF);
  }
  if (unix_socket_path) {
    if (-1 == chmod(unix_socket_path, 0777)) {
      perror("Failed to chmod socket");
      exit(EXIT_CONF);
    }
  }

  if (-1 == listen(skt, SERVER_LISTEN_BACKLOG)) {
    perror("Failed to listen on socket");
    exit(EXIT_INTERNAL);
  }
  DEBUG_TS_LOG("Listening");

  int epfd = epoll_create1(0);
  if (-1 == epfd) {
    perror("Failed to create epoll");
    exit(EXIT_INTERNAL);
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = skt;
  if (-1 == epoll_ctl(epfd, EPOLL_CTL_ADD, skt, &ev)) {
    perror("Failed to add socket to epoll");
    exit(EXIT_INTERNAL);
  }

  server_t* svr = malloc(sizeof(server_t));
  svr->clients = server_clients_create();
  svr->socket_fd = skt;
  svr->epoll_fd = epfd;
  svr->method_ctx = method_ctx;
  // NULL is not guaranteed to be 0, so cannot simply use calloc.
  for (int i = 0; i < 256; i++) {
    svr->methods.state_initialisers[i] = NULL;
    svr->methods.handlers[i] = NULL;
  }
  svr->methods.state_initialisers[METHOD_COMMIT_OBJECT] = method_commit_object_state_init;
  svr->methods.handlers[METHOD_COMMIT_OBJECT] = method_commit_object;
  svr->methods.state_initialisers[METHOD_CREATE_OBJECT] = method_create_object_state_init;
  svr->methods.handlers[METHOD_CREATE_OBJECT] = method_create_object;
  svr->methods.state_initialisers[METHOD_DELETE_OBJECT] = method_delete_object_state_init;
  svr->methods.handlers[METHOD_DELETE_OBJECT] = method_delete_object;
  svr->methods.state_initialisers[METHOD_INSPECT_OBJECT] = method_inspect_object_state_init;
  svr->methods.handlers[METHOD_INSPECT_OBJECT] = method_inspect_object;
  svr->methods.state_initialisers[METHOD_READ_OBJECT] = method_read_object_state_init;
  svr->methods.handlers[METHOD_READ_OBJECT] = method_read_object;
  svr->methods.state_initialisers[METHOD_WRITE_OBJECT] = method_write_object_state_init;
  svr->methods.handlers[METHOD_WRITE_OBJECT] = method_write_object;
  return svr;
}

static inline svr_client_result_t server_process_client_until_result(server_t* server, svr_client_t* client) {
  while (true) {
    int fd = client->fd;
    svr_method_args_parser_t* ap = &client->args_parser;
    method_t method = client->method;
    if (method == METHOD__UNKNOWN) {
      // We haven't parsed the args yet.
      int readlen = maybe_read(fd, ap->raw + ap->write_next, 255 - ap->write_next);
      if (!readlen) { \
        return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;
      }
      if (readlen < 0) {
        return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
      }
      if ((ap->write_next += readlen) == 255) {
        ap->raw_len = ap->raw[0];
        // We'll validdate this when we get `init_fn`.
        client->method = ap->raw[1];
        ap->read_next = 2;
        // Parse the args.
        server_method_state_initialiser* init_fn = server->methods.state_initialisers[method];
        if (init_fn == NULL) {
          return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
        }
        init_fn(&client->method_state, server->method_ctx, ap);
      }
    } else {
      // The method must exist.
      server_method_handler* fn = server->methods.handlers[method];
      return fn(server->method_ctx, &client->method_state, client);
    }
  }
}

static void on_client_event(server_t* server, svr_client_t* client) {
  while (true) {
    svr_client_result_t res = server_process_client_until_result(server, client);

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE) {
      server_rearm_client_to_epoll(server, client, true, false);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE) {
      server_rearm_client_to_epoll(server, client, false, true);
      break;
    }

    if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH) {
      break;
    }

    if (res == SVR_CLIENT_RESULT_END) {
      server_client_reset(client);
      continue;
    }

    if (res == SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR) {
      server_clients_close(server->clients, client);
      break;
    }

    fprintf(stderr, "Unknown client (method=%d) action result: %d\n", client->method, res);
    exit(EXIT_INTERNAL);
  }
}

void server_wait_epoll(server_t* server, int timeout) {
  struct epoll_event svr_epoll_events[SERVER_EPOLL_EVENTS_MAX];
  int nfds = epoll_wait(server->epoll_fd, svr_epoll_events, SERVER_EPOLL_EVENTS_MAX, timeout);
  if (-1 == nfds) {
    if (errno == EINTR) {
      // Coz will frequently interrupt the process with signals; just ignore them.
      return;
    }
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

      svr_client_t* client = server_clients_add(server->clients, peer);

      // Add to epoll.
      struct epoll_event ev;
      ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET;
      ev.data.ptr = client;
      if (-1 == epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, peer, &ev)) {
        perror("Failed to add connection to epoll");
        exit(EXIT_INTERNAL);
      }
    } else {
      on_client_event(server, svr_epoll_events[n].data.ptr);
    }
  }
}

void server_rearm_client_to_epoll(server_t* server, svr_client_t* client, bool read, bool write) {
  struct epoll_event ev;
  ev.events = EPOLLET | EPOLLONESHOT | (read ? EPOLLIN : 0) | (write ? EPOLLOUT : 0);
  ev.data.ptr = client;
  if (-1 == epoll_ctl(server->epoll_fd, EPOLL_CTL_MOD, client->fd, &ev)) {
    perror("Failed to add connection to epoll");
    exit(EXIT_INTERNAL);
  }
}
