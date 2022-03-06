#pragma once

#include "server.h"

typedef struct manager_state_s manager_state_t;

typedef struct manager_s manager_t;

manager_state_t* manager_state_create();

void manager_hand_off_client(
  manager_state_t* manager_state,
  server_t* server,
  svr_client_t* client
);

manager_t* manager_create(manager_state_t* manager_state, flush_state_t* flush_state, server_t* server);

void manager_start(manager_t* mgr);
