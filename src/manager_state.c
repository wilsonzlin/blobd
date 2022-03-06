#include "lossy_mpsc_queue.h"
#include "manager_state.h"

manager_state_t* manager_state_create() {
  manager_state_t* st = malloc(sizeof(manager_state_t));
  st->client_queue = lossy_mpsc_queue_create(20);
  return st;
}

svr_client_t* manager_hand_off_client(
  manager_state_t* manager_state,
  svr_client_t* client
) {
  svr_client_t* popped = lossy_mpsc_queue_enqueue(manager_state->client_queue, client);
  return popped;
}
