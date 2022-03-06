#pragma once

#include "flush.h"
#include "manager_state.h"
#include "server.h"

typedef struct manager_s manager_t;

manager_t* manager_create(manager_state_t* manager_state, flush_state_t* flush_state, server_t* server);

void manager_start(manager_t* mgr);
