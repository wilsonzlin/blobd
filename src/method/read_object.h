#pragma once

#include "../server_client.h"

method_error_t method_read_object_parse(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  uint8_t* args_cur
);

svr_client_result_t method_read_object_response(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
);

svr_client_result_t method_read_object_postresponse(
  method_ctx_t* ctx,
  method_read_object_state_t* state,
  svr_client_t* client
);
