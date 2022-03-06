#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../ext/klib/khash.h"
#include "server_client.h"

KHASH_MAP_INIT_INT(fd_to_client, svr_client_t*);

struct server_clients_s {
  kh_fd_to_client_t* fd_to_client;
};

server_clients_t* server_clients_create() {
  server_clients_t* clients = malloc(sizeof(server_clients_t));
  clients->fd_to_client = kh_init_fd_to_client();
  return clients;
}

svr_client_t* server_clients_add(server_clients_t* clients, int client_fd) {
  svr_client_t* client = malloc(sizeof(svr_client_t));
  client->fd = client_fd;
  client->args_parser = NULL;
  client->method = METHOD__UNKNOWN;
  client->method_state = NULL;

  int kh_res;
  khint_t kh_it = kh_put_fd_to_client(clients->fd_to_client, client_fd, &kh_res);
  if (-1 == kh_res) {
    fprintf(stderr, "Failed to insert client into map\n");
    exit(EXIT_INTERNAL);
  }
  kh_val(clients->fd_to_client, kh_it) = client;

  return client;
}

svr_client_t* server_clients_get(server_clients_t* clients, int client_fd) {
  khint_t kh_it = kh_get_fd_to_client(clients->fd_to_client, client_fd);
  if (kh_it == kh_end(clients->fd_to_client)) {
    fprintf(stderr, "Client does not exist\n");
    exit(EXIT_INTERNAL);
  }
  return kh_val(clients->fd_to_client, kh_it);
}

void server_clients_close(server_clients_t* clients, svr_client_t* client) {
  if (-1 == close(client->fd)) {
    perror("Failed to close client FD");
    exit(EXIT_INTERNAL);
  }
  khint_t k = kh_get_fd_to_client(clients->fd_to_client, client->fd);
  if (k == kh_end(clients->fd_to_client)) {
    fprintf(stderr, "Client does not exist\n");
    exit(EXIT_INTERNAL);
  }
  kh_del_fd_to_client(clients->fd_to_client, k);

  // Destroy the client.
  if (client->args_parser != NULL) {
    server_method_args_parser_destroy(client->args_parser);
  }
  if (client->method_state != NULL) {
    client->method_state_destructor(client->method_state);
  }
  free(client);
}
