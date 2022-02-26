#pragma once

#include "../server.h"

typedef struct method_create_object_state_t;

method_create_object_state_t* method_create_object_state_create(
  svr_method_handler_ctx_t* ctx,
  svr_method_args_parser_t* parser
);

void method_create_object_state_destroy(method_create_object_state_t* args);

svr_client_result_t method_create_object(
  svr_method_handler_ctx_t* ctx,
  method_create_object_state_t* state
);
