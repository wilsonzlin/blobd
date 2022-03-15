#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "log.h"
#include "server_client.h"

LOGGER("server_client");

struct server_clients_s {
  _Atomic(svr_client_t*) pool_head;
};

// Don't clear FD as we're resetting for the same client.
void server_client_reset(svr_client_t* client) {
  client->args_recvd = 0;
  client->method = METHOD__UNKNOWN;
  client->res_len = 0;
  client->res_sent = -1;
}

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
    // We align by 64 as most method state structs contain a __m512i.
    ASSERT_ERROR_RETVAL_OK(posix_memalign((void**) &client, 64, sizeof(svr_client_t)), "alloc client");
    atomic_init(&client->next_free_in_pool, NULL);
  }
  server_client_reset(client);
  client->fd = client_fd;

  return client;
}

void server_clients_close(server_clients_t* clients, svr_client_t* client) {
  if (-1 == close(client->fd)) {
    perror("Failed to close client FD");
    exit(EXIT_INTERNAL);
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
