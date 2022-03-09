#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "server_client.h"

// This is opaque to ensure proper atomic reads and writes, and consideration of pooling.
struct svr_client_s {
  // This doesn't need to be atomic; only one thread can ever see/touch/manage a svr_client_t at one time.
  int fd;
  svr_method_args_parser_t* args_parser;
  method_t method;
  void* method_state;
  method_state_destructor_t* method_state_destructor;
  // For server_client.h internal use only.
  _Atomic(struct svr_client_s*) next_free_in_pool;
};

int server_client_get_fd(svr_client_t* client) {
  return client->fd;
}

svr_method_args_parser_t* server_client_get_args_parser(svr_client_t* client) {
  return client->args_parser;
}

void server_client_set_args_parser(svr_client_t* client, svr_method_args_parser_t* args_parser) {
  client->args_parser = args_parser;
}

method_t server_client_get_method(svr_client_t* client) {
  return client->method;
}

void server_client_set_method(svr_client_t* client, method_t method) {
  client->method = method;
}

void* server_client_get_method_state(svr_client_t* client) {
  return client->method_state;
}

void server_client_set_method_state(svr_client_t* client, void* method_state) {
  client->method_state = method_state;
}

void server_client_set_method_state_destructor(svr_client_t* client, method_state_destructor_t* destructor) {
  client->method_state_destructor = destructor;
}

void server_client_reset(svr_client_t* client) {
  client->method = METHOD__UNKNOWN;
  if (client->args_parser != NULL) {
    server_method_args_parser_destroy(client->args_parser);
    client->args_parser = NULL;
  }
  if (client->method_state != NULL) {
    client->method_state_destructor(client->method_state);
    client->method_state = NULL;
    client->method_state_destructor = NULL;
  }
}

struct server_clients_s {
  _Atomic(svr_client_t*) pool_head;
};

server_clients_t* server_clients_create() {
  server_clients_t* clients = malloc(sizeof(server_clients_t));
  atomic_init(&clients->pool_head, NULL);
  return clients;
}

svr_client_t* server_clients_add(server_clients_t* clients, int client_fd) {
  svr_client_t* client;
  do {
    client = atomic_load_explicit(&clients->pool_head, memory_order_relaxed);
  } while (
    client != NULL &&
    !atomic_compare_exchange_weak_explicit(
      &clients->pool_head,
      &client,
      atomic_load_explicit(&client->next_free_in_pool, memory_order_relaxed),
      memory_order_relaxed,
      memory_order_relaxed
    )
  );
  if (client == NULL) {
    client = malloc(sizeof(svr_client_t));
    atomic_init(&client->next_free_in_pool, NULL);
  }
  client->fd = client_fd;
  client->args_parser = NULL;
  client->method = METHOD__UNKNOWN;
  client->method_state = NULL;
  client->method_state_destructor = NULL;

  return client;
}

void server_clients_close(server_clients_t* clients, svr_client_t* client) {
  if (-1 == close(client->fd)) {
    perror("Failed to close client FD");
    exit(EXIT_INTERNAL);
  }

  // Destroy the client state.
  if (client->args_parser != NULL) {
    server_method_args_parser_destroy(client->args_parser);
  }
  if (client->method_state != NULL) {
    client->method_state_destructor(client->method_state);
  }

  svr_client_t* old_head = atomic_load_explicit(&clients->pool_head, memory_order_relaxed);
  do {
    atomic_store_explicit(&client->next_free_in_pool, old_head, memory_order_relaxed);
  } while (!atomic_compare_exchange_weak_explicit(
    &clients->pool_head,
    &old_head,
    client,
    memory_order_relaxed,
    memory_order_relaxed
  ));
}
