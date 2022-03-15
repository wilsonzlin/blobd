#pragma once

#include <stdint.h>

/**

CONF
====

The configuration file has the following format:

each_line_has_a_key and_a_value
a_key_can_have_a_number_value 1384234
string_values_do_not_need_quoting /a/long/path/with spaces/in the middle
numerical_values_can_have_decimals 3.141592653589793

**/

typedef struct {
  // Only used when formatting.
  uint64_t bucket_count;

  char* device_path;

  char* worker_address;
  uint16_t worker_port;
  uint16_t worker_threads;
  char* worker_unix_socket_path;
} conf_t;

typedef struct conf_parser_s conf_parser_t;

conf_parser_t* conf_parser_create();

void conf_parser_destroy(conf_parser_t* parser);

conf_t* conf_parse(conf_parser_t* parser, char* raw, uint64_t len);

void conf_destroy(conf_t* conf);
