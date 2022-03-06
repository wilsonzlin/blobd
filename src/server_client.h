#pragma once

#include "method/_common.h"
#include "server_method_args.h"

typedef enum {
  SVR_CLIENT_RESULT__UNKNOWN,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_READABLE,
  SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE,
  SVR_CLIENT_RESULT_AWAITING_FLUSH,
  SVR_CLIENT_RESULT_HANDED_OFF_TO_MANAGER,
  SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR,
  SVR_CLIENT_RESULT_END,
} svr_client_result_t;

typedef struct {
  int fd;
  svr_method_args_parser_t* args_parser;
  method_t method;
  void* method_state;
  void (*method_state_destructor)(void*);
  // Set only if the manager previously handled the client and has now handed it back to the server.
  svr_client_result_t method_result_from_manager;
  // If we've already handed it off to the manager worker, don't hand it off again.
  bool already_processed_by_manager;
} svr_client_t;
