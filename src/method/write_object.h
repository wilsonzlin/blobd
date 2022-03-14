#pragma once

#include "../server_client.h"

method_error_t method_write_object_parse(
  method_ctx_t* ctx,
  method_write_object_state_t* state,
  uint8_t* args_raw
);

svr_client_result_t method_write_object_response(
  method_ctx_t* ctx,
  method_write_object_state_t* state,
  svr_client_t* client,
  uint8_t* out_response
);
