#pragma once

#include "lossy_mpsc_queue.h"
#include "server_client.h"

typedef struct {
  lossy_mpsc_queue_t* client_queue;
} manager_state_t;

manager_state_t* manager_state_create();

// May return a non-NULL pointer to a client that needs to be destroyed.
svr_client_t* manager_hand_off_client(
  manager_state_t* manager_state,
  svr_client_t* client
);
