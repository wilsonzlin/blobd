#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "server_client.h"

// Must be power of 2.
#define SERVER_CLIENTS_BUCKETS 1048576

// This is opaque to ensure proper atomic reads and writes, and consideration of pooling.
struct svr_client_s {
  // This must be atomic to prevent partial reads, even if use atomic reads and memory barriers when reading next_* fields.
  _Atomic(int) fd;
  svr_method_args_parser_t* args_parser;
  method_t method;
  void* method_state;
  method_state_destructor_t* method_state_destructor;
  // For server_client.h internal use only.
  _Atomic(struct svr_client_s*) next_in_bucket;
};

int server_client_get_fd(svr_client_t* client) {
  // Use memory_order_acquire as it's likely caller will read other nonatomic fields of svr_client_t.
  return atomic_load_explicit(&client->fd, memory_order_acquire);
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
  _Atomic(svr_client_t*) buckets[SERVER_CLIENTS_BUCKETS];
};

server_clients_t* server_clients_create() {
  server_clients_t* clients = malloc(sizeof(server_clients_t));
  atomic_init(&clients->pool_head, NULL);
  for (uint64_t i = 0; i < SERVER_CLIENTS_BUCKETS; i++) {
    atomic_init(&clients->buckets[i], NULL);
  }
  return clients;
}

// We can make some guarantees:
// - We only ever prepend to a bucket's list.
// - Only one thread can be deleting a node.
// However, this still makes deletions difficult, especially given that we're using a pool,
// which means any multistep operation must also ensure that we aren't reading a new value of a reused svr_client_t.
// Therefore, for simplicity, we never remove from a bucket (and therefore don't use a pool), only marking nodes as unused. This should work fine if there is never too many connections at one time, and FDs are allocated sequentially (we can use a hash function if not). Usually this means that there would only be a few (less than 5) comparisons per lookup. It might also be faster if there are lots of short-lived connections, due to lack of need to delete from lists and release into pools.
svr_client_t* server_clients_add(server_clients_t* clients, int client_fd) {
  uint64_t bkt = client_fd & (SERVER_CLIENTS_BUCKETS - 1);
  svr_client_t* client = atomic_load_explicit(&clients->buckets[bkt], memory_order_relaxed);
  while (client != NULL) {
    int existing_fd = atomic_load_explicit(&client->fd, memory_order_relaxed);
    if (existing_fd == -1 && atomic_compare_exchange_strong_explicit(&client->fd, &existing_fd, client_fd, memory_order_relaxed, memory_order_relaxed)) {
      break;
    }
    client = atomic_load_explicit(&client->next_in_bucket, memory_order_relaxed);
  }
  if (client == NULL) {
    client = malloc(sizeof(svr_client_t));
    atomic_init(&client->next_in_bucket, NULL);
    svr_client_t* cur_head = atomic_load_explicit(&clients->buckets[bkt], memory_order_relaxed);
    do {
      atomic_store_explicit(&client->next_in_bucket, cur_head, memory_order_relaxed);
    } while (!atomic_compare_exchange_weak_explicit(
      &clients->buckets[bkt],
      &cur_head,
      client,
      memory_order_relaxed,
      memory_order_relaxed
    ));
  }
  client->fd = client_fd;
  client->args_parser = NULL;
  client->method = METHOD__UNKNOWN;
  client->method_state = NULL;
  client->method_state_destructor = NULL;

  return client;
}

svr_client_t* server_clients_get(server_clients_t* clients, int client_fd) {
  uint64_t bkt = client_fd & (SERVER_CLIENTS_BUCKETS - 1);

  svr_client_t* client = atomic_load_explicit(&clients->buckets[bkt], memory_order_relaxed);
  // Use memory_order_acquire as it's likely caller will read other nonatomic fields of svr_client_t.
  while (client != NULL && atomic_load_explicit(&client->fd, memory_order_acquire) != client_fd) {
    client = atomic_load_explicit(&client->next_in_bucket, memory_order_relaxed);
  }
  if (client == NULL) {
    fprintf(stderr, "Client does not exist\n");
    exit(EXIT_INTERNAL);
  }
  return client;
}

void server_clients_close(svr_client_t* client) {
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

  atomic_store_explicit(&client->fd, -1, memory_order_relaxed);
}
