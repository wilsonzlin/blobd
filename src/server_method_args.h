#pragma once

#include <stdbool.h>
#include <stdint.h>

typedef struct {
  uint16_t read_next;
  uint16_t write_next;
  uint8_t raw_len;
  uint8_t raw[255];
} svr_method_args_parser_t;

void server_method_args_parser_reset(svr_method_args_parser_t* p);

// Returns NULL if not enough bytes.
uint8_t* svr_method_args_parser_parse(svr_method_args_parser_t* parser, uint64_t want_bytes);

// Returns false if not all argument bytes were used.
bool svr_method_args_parser_end(svr_method_args_parser_t* parser);
