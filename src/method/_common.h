#pragma once

#include <errno.h>
#include <immintrin.h>
#include <stdint.h>
#include "../bucket.h"
#include "../cursor.h"
#include "../device.h"
#include "../freelist.h"
#include "../inode.h"
#include "../server_method_args.h"
#include "../stream.h"
#include "../util.h"

typedef struct {
  buckets_t* bkts;
  device_t* dev;
  // This is flush_state_t*, but we use void* to avoid cyclic imports. Note that it's impossible to avoid regardless of how we organise the files, since the server depends on this, and flush_state_t depends on the server.
  void* flush_state;
  freelist_t* fl;
  stream_t* stream;
} method_ctx_t;

typedef enum {
  // Dummy value.
  METHOD__UNKNOWN = 0,
  METHOD_CREATE_OBJECT = 1,
  METHOD_INSPECT_OBJECT = 2,
  METHOD_READ_OBJECT = 3,
  METHOD_WRITE_OBJECT = 4,
  METHOD_COMMIT_OBJECT = 5,
  METHOD_DELETE_OBJECT = 6,
} method_t;

typedef union {
  __m512i vecs[2];
  uint8_t bytes[129];
} method_common_key_data_t;

typedef struct {
  method_common_key_data_t data __attribute__((aligned (64)));
  uint8_t len;
  uint64_t bucket;
} method_common_key_t;

typedef enum {
  METHOD_ERROR_OK = 0,
  METHOD_ERROR_NOT_ENOUGH_ARGS = 1,
  METHOD_ERROR_KEY_TOO_LONG = 2,
  METHOD_ERROR_TOO_MANY_ARGS = 3,
  METHOD_ERROR_NOT_FOUND = 4,
  METHOD_ERROR_INVALID_START = 5,
  METHOD_ERROR_INVALID_END = 6,
} method_error_t;

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  buckets_t* bkts,
  method_common_key_t* out
);

// `b` must be filled with 0.
bool compare_raw_key_with_vec_key(uint8_t* a, uint8_t a_len, method_common_key_data_t b, uint8_t b_len);

uint64_t method_common_find_inode_in_bucket(
  device_t* dev,
  buckets_t* buckets,
  method_common_key_t* key,
  ino_state_t allowed_states,
  uint64_t required_obj_no_or_zero,
  uint64_t* out_prev_inode_dev_offset_or_null
);

#define METHOD_COMMIT_OBJECT_RESPONSE_LEN 1

typedef struct {
  int response_written;
  uint8_t response[METHOD_COMMIT_OBJECT_RESPONSE_LEN];
  uint64_t inode_dev_offset;
  uint64_t obj_no;
} method_commit_object_state_t;

#define METHOD_CREATE_OBJECT_RESPONSE_LEN 9

typedef struct {
  int response_written;
  // [u8 error, u64 obj_no].
  uint8_t response[METHOD_CREATE_OBJECT_RESPONSE_LEN];
  method_common_key_t key;
  uint64_t size;
} method_create_object_state_t;

#define METHOD_DELETE_OBJECT_RESPONSE_LEN 1

typedef struct {
  int response_written;
  uint8_t response[METHOD_DELETE_OBJECT_RESPONSE_LEN];
  method_common_key_t key;
  uint64_t obj_no_or_zero;
} method_delete_object_state_t;

// [u8 error, u8 state, u64 size].
#define METHOD_INSPECT_OBJECT_RESPONSE_LEN (1 + 1 + 8)

typedef struct {
  // -1 if not prepared yet.
  int response_written;
  uint8_t response[METHOD_INSPECT_OBJECT_RESPONSE_LEN];
  method_common_key_t key;
} method_inspect_object_state_t;

// [u8 error, u64 actual_read_start, u64 actual_read_len, u64 obj_size].
#define METHOD_READ_OBJECT_RESPONSE_LEN (1 + 8 + 8 + 8)

typedef struct {
  uint32_t read_count;
  // 0 if not found yet.
  uint64_t obj_no;
  // -1 if not prepared yet.
  int response_written;
  uint8_t response[METHOD_READ_OBJECT_RESPONSE_LEN];
  method_common_key_t key;
  int64_t arg_start;
  int64_t arg_end;
  // These are undefiend if not found yet.
  uint64_t actual_start;
  uint64_t actual_length;
  uint64_t object_size;
} method_read_object_state_t;

#define METHOD_WRITE_OBJECT_RESPONSE_LEN 1

typedef struct {
  uint32_t written;
  int response_written;
  uint8_t response[METHOD_WRITE_OBJECT_RESPONSE_LEN];
  method_common_key_t key;
  uint64_t obj_no;
  uint64_t start;
} method_write_object_state_t;

// To avoid cyclic dependencies, define this union as well as all member structs in this file.
typedef union {
  method_commit_object_state_t commit_object;
  method_create_object_state_t create_object;
  method_delete_object_state_t delete_object;
  method_inspect_object_state_t inspect_object;
  method_read_object_state_t read_object;
  method_write_object_state_t write_object;
} method_state_t;

#define INIT_STATE_RESPONSE(state, response_len) \
  memset(state->response, 0, response_len); \
  state->response_written = -1

#define PARSE_ERROR(state, error) \
  state->response_written = 0; \
  state->response[0] = error; \
  return;

#define ERROR_RESPONSE(error) \
  args->response[0] = error; \
  args->response_written = 0; \
  return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE;

#define MAYBE_HANDLE_RESPONSE(state, response_len, client, return_on_end) \
  if (state->response_written >= 0 && state->response_written < response_len) { \
    int maybehandleresponsewriteresult = maybe_write(client->fd, state->response + state->response_written, response_len - state->response_written); \
    if (-1 == maybehandleresponsewriteresult) { \
      return SVR_CLIENT_RESULT_UNEXPECTED_EOF_OR_IO_ERROR; \
    } \
    if (0 == maybehandleresponsewriteresult) { \
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
    } \
    if ((state->response_written += maybehandleresponsewriteresult) < response_len) { \
      return SVR_CLIENT_RESULT_AWAITING_CLIENT_WRITABLE; \
    } \
    if (return_on_end) { \
      return SVR_CLIENT_RESULT_END; \
    } \
  }
