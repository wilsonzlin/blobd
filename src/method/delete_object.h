#pragma once

#include "../manager.h"
#include "../server_client.h"
#include "../server_method_args.h"

typedef struct method_delete_object_state_s method_delete_object_state_t;

void* method_delete_object_state_create(
  void* ctx_raw,
  svr_method_args_parser_t* parser
);

void method_delete_object_state_destroy(void* args);

svr_client_result_t method_delete_object(
  void* ctx_raw,
  void* state_raw,
  int client_fd
);
