#pragma once

#include <immintrin.h>
#include <stdint.h>
#include "../server.h"

typedef enum {
  METHOD_ERROR_OK,
  METHOD_ERROR_NOT_ENOUGH_ARGS,
  METHOD_ERROR_KEY_TOO_LONG,
  METHOD_ERROR_TOO_MANY_ARGS,
  METHOD_ERROR_NOT_FOUND,
} method_error_t;

#define INIT_STATE_RESPONSE(state, response_len) \
  memset(state->response, 0, response_len); \
  state->response_written = -1

#define PARSE_ERROR(state, error) \
  state->response_written = 0; \
  state->response[0] = error; \
  return state;

#define MAYBE_HANDLE_RESPONSE(state, response_len, client_fd) \
  if (state->response_written >= 0) { \
    int maybehandleresponsewriteresult = write(client_fd, state->response + state->response_written, response_len - state->response_written); \
    if (-1 == maybehandleresponsewriteresult && (errno == EAGAIN || errno == EWOULDBLOCK)) { \
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
    } \
    if (maybehandleresponsewriteresult > 0) { \
      if ((state->response_written += maybehandleresponsewriteresult) == RESPONSE_LEN) { \
        return SVR_CLIENT_RESULT_END; \
      } else { \
        return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
      } \
    } \
    return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
  }

typedef union {
  __m512i vecs[2];
  uint8_t bytes[129];
} method_common_key_data_t;

typedef struct {
  method_common_key_data_t data;
  uint8_t len;
  uint64_t bucket;
} method_common_key_t;

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  uint8_t bucket_count_log2,
  method_common_key_t* out
);
