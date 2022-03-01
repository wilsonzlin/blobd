#include "../server.h"
#include "_common.h"
#include "../../ext/xxHash/xxhash.h"

method_error_t method_common_key_parse(
  svr_method_args_parser_t* parser,
  uint8_t bucket_count_log2,
  method_common_key_t* out
) {
  uint8_t* p = NULL;

  if ((p = svr_method_args_parser_parse(parser, 1)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  if ((out->len = *p) > 128) return METHOD_ERROR_KEY_TOO_LONG;
  if ((p = svr_method_args_parser_parse(parser, out->len)) == NULL) return METHOD_ERROR_NOT_ENOUGH_ARGS;
  memcpy(out->data.bytes, p, out->len);
  memset(out->data.bytes + out->len, 0, 129 - out->len);
  out->bucket = XXH3_64bits(out->data.bytes, out->len) & ((1llu << bucket_count_log2) - 1);

  return METHOD_ERROR_OK;
}
