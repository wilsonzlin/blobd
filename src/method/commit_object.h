#pragma once

#include "../server.h"

typedef struct method_commit_object_state_s method_commit_object_state_t;

method_commit_object_state_t* method_commit_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
);

void method_commit_object_state_destroy(void* args);

svr_client_result_t method_commit_object(
  svr_method_handler_ctx_t* ctx,
  method_commit_object_state_t* state,
  int client_fd
);
