#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include "server_method_args.h"

svr_method_args_parser_t* server_method_args_parser_create(uint64_t raw_len) {
  svr_method_args_parser_t* p = malloc(sizeof(svr_method_args_parser_t) + raw_len);
  p->read_next = 0;
  p->write_next = 0;
  p->raw_len = raw_len;
  return p;
}

void server_method_args_parser_destroy(svr_method_args_parser_t* p) {
  free(p);
}

uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, uint64_t want_bytes) {
  if (parser->read_next + want_bytes > parser->raw_len) {
    return NULL;
  }
  uint8_t* rv = parser->raw + parser->read_next;
  parser->read_next += want_bytes;
  return rv;
}

bool svr_method_args_parser_end(svr_method_args_parser_t* parser) {
  return parser->read_next == parser->raw_len;
}
