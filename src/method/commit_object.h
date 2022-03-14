#pragma once

#include "../server_client.h"
#include "../worker.h"

method_error_t method_commit_object_parse(
  method_ctx_t* ctx,
  method_commit_object_state_t* state,
  uint8_t* args_cur
);

svr_client_result_t method_commit_object_response(
  method_ctx_t* ctx,
  method_commit_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
);
