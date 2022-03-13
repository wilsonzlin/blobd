#pragma once

#include "../server_client.h"
#include "../server_method_args.h"
#include "../worker.h"

void method_commit_object_state_init(
  void* state_raw,
  method_ctx_t* ctx,
  svr_method_args_parser_t* parser
);

svr_client_result_t method_commit_object(
  method_ctx_t* ctx,
  void* state_raw,
  svr_client_t* client
);
