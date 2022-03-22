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

#define SERVER_EPOLL_EVENTS_MAX 65536
#define SERVER_LISTEN_BACKLOG 1024

LOGGER("server");

struct server_s {
  server_clients_t* clients;
  int socket_fd;
  int epoll_fd;
  method_ctx_t* method_ctx;
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
  return svr;
}

#define CALL_PARSER(response_len, method) { \
    client->res_len = response_len; \
    method_error_t err = method_##method##_parse(server->method_ctx, &client->method_state.method, client->buf + 1); \
    memset(client->buf, 0, response_len); \
    if (err != METHOD_ERROR_OK) { \
      client->buf[0] = err; \
      client->res_sent = 0; \
    } \
  } \
  break \

#define CALL_HANDLER(method) { \
    svr_client_result_t res = method_##method##_response(server->method_ctx, &client->method_state.method, client, client->buf); \
    if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE || res == SVR_CLIENT_RESULT_WRITE_RESPONSE) { \
      client->res_sent = 0; \
    } \
    if (res != SVR_CLIENT_RESULT_WRITE_RESPONSE) { \
      return res; \
    } \
  } \
  break

static inline svr_client_result_t server_process_client_until_result(server_t* server, svr_client_t* client) {
  while (true) {
    if (client->method == METHOD__UNKNOWN) {
      // We haven't parsed the args yet.
      int readlen = maybe_read(client->fd, client->buf + client->args_recvd, 255 - client->args_recvd);
      if (readlen < 0) {
        return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
      }
      if ((client->args_recvd += readlen) < 255) {
        return SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE;
      }
      // Parse the args.
      switch ((client->method = client->buf[0])) {
      case METHOD_COMMIT_OBJECT: CALL_PARSER(METHOD_COMMIT_OBJECT_RESPONSE_LEN, commit_object);
      case METHOD_CREATE_OBJECT: CALL_PARSER(METHOD_CREATE_OBJECT_RESPONSE_LEN, create_object);
      case METHOD_DELETE_OBJECT: CALL_PARSER(METHOD_DELETE_OBJECT_RESPONSE_LEN, delete_object);
      case METHOD_INSPECT_OBJECT: CALL_PARSER(METHOD_INSPECT_OBJECT_RESPONSE_LEN, inspect_object);
      case METHOD_READ_OBJECT: CALL_PARSER(METHOD_READ_OBJECT_RESPONSE_LEN, read_object);
      case METHOD_WRITE_OBJECT: CALL_PARSER(METHOD_WRITE_OBJECT_RESPONSE_LEN, write_object);
      default:
        ts_log(WARN, "Invalid request method: %d", client->method);
        return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
      }
    } else if (client->res_sent == -1) {
      // We need to keep calling the method handler.
      switch (client->method) {
      case METHOD_COMMIT_OBJECT: CALL_HANDLER(commit_object);
      case METHOD_CREATE_OBJECT: CALL_HANDLER(create_object);
      case METHOD_DELETE_OBJECT: CALL_HANDLER(delete_object);
      case METHOD_INSPECT_OBJECT: CALL_HANDLER(inspect_object);
      case METHOD_READ_OBJECT: CALL_HANDLER(read_object);
      case METHOD_WRITE_OBJECT: CALL_HANDLER(write_object);
      default: ASSERT_UNREACHABLE("method %d", client->method);
      }
    } else if (client->res_sent >= 0 && client->res_sent < client->res_len) {
      int writeno = maybe_write(client->fd, client->buf + client->res_sent, client->res_len - client->res_sent);
      if (writeno < 0) {
        return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR;
      }
      if ((client->res_sent += writeno) < client->res_len) {
        return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;
      }
      if (client->method != METHOD_READ_OBJECT) {
        return SVR_CLIENT_RESULT_END;
      }
    } else {
      ASSERT_STATE(client->method == METHOD_READ_OBJECT, "method %d does not have postresponse", client->method);
      return method_read_object_postresponse(server->method_ctx, &client->method_state.read_object, client);
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

    if (res == SVR_CLIENT_RESULT_AWAITING_FLUSH_THEN_WRITE_RESPONSE) {
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
