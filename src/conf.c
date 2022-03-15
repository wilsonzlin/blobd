#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "../ext/klib/khash.h"
#include "conf.h"
#include "exit.h"
#include "util.h"

typedef enum {
  FT_STR,
  FT_U16,
  FT_U64,
} field_type_t;

typedef struct {
  field_type_t type;
  size_t offset;
  uint64_t min;
  uint64_t max;
} field_parser_t;

KHASH_MAP_INIT_STR(field_parsers, field_parser_t);

struct conf_parser_s {
  kh_field_parsers_t* fields;
  uint64_t key_len_min;
  uint64_t key_len_max;
};

#define STRINGIFY(x) #x

#define FIELD(name, typ, min_val, max_val) \
  kh_it = kh_put_field_parsers(parser->fields, STRINGIFY(name), &kh_ret); \
  if (-1 == kh_ret) { \
    fprintf(stderr, "Failed to insert into parser map\n"); \
    exit(EXIT_INTERNAL); \
  } \
  kh_val(parser->fields, kh_it) = (field_parser_t) {typ, offsetof(conf_t, name), min_val, max_val}; \
  parser->key_len_min = min(parser->key_len_min, sizeof(STRINGIFY(name)) - 1); \
  parser->key_len_max = max(parser->key_len_max, sizeof(STRINGIFY(name)) - 1) \

conf_parser_t* conf_parser_create() {
  conf_parser_t* parser = malloc(sizeof(conf_parser_t));
  parser->fields = kh_init_field_parsers();
  parser->key_len_max = 0;
  parser->key_len_min = UINT64_MAX;
  int kh_ret;
  khint_t kh_it;
  FIELD(bucket_count, FT_U64, 4096llu, 281474976710656llu);
  FIELD(device_path, FT_STR, 1, 4096);
  FIELD(worker_address, FT_STR, 1, 4096);
  FIELD(worker_port, FT_U16, 1, 65535);
  FIELD(worker_threads, FT_U16, 1, 65535);
  FIELD(worker_unix_socket_path, FT_STR, 1, 4096);
  return parser;
}

void conf_parser_destroy(conf_parser_t* parser) {
  kh_destroy_field_parsers(parser->fields);
  free(parser);
}

static inline void invalid_key(char* raw, uint64_t len) {
  for (uint64_t i = 0; i < len; i++) fputc(raw[i], stderr);
  fprintf(stderr, " is not a valid key or does not have a valid value\n");
  exit(EXIT_CONF);
}

conf_t* conf_parse(conf_parser_t* parser, char* raw, uint64_t len) {
  conf_t* conf = malloc(sizeof(conf_t));
  char* conf_raw = (char*) conf;

  conf->bucket_count = 0;

  conf->device_path = NULL;

  conf->worker_address = NULL;
  conf->worker_port = 0;
  conf->worker_threads = 0;
  conf->worker_unix_socket_path = NULL;

  char* key_buf = malloc(parser->key_len_max + 1);
  uint64_t i = 0;
  while (i < len) {
    while (i < len && isspace(raw[i])) i++;
    // At this point, `i` is either at a nonspace or EOF.
    uint64_t j = i + 1; while (j < len && !isspace(raw[j])) j++;
    // At this point, `j` is either at a space, EOF, or past EOF.
    if (j > len) break;
    char* key = raw + i; uint64_t key_len = j - i;
    if (key_len < parser->key_len_min || key_len > parser->key_len_max) invalid_key(key, key_len);
    memcpy(key_buf, key, key_len);
    key_buf[key_len] = 0;
    khint_t it = kh_get_field_parsers(parser->fields, key_buf);
    if (it == kh_end(parser->fields)) invalid_key(key, key_len);
    field_parser_t f = kh_val(parser->fields, it);
    i = j;
    // At this point, `i` is either at a space or EOF.

    while (i < len && isspace(raw[i])) i++;
    // At this point, `i` is either at a nonspace or EOF.
    j = i + 1; while (j < len && raw[j] != '\n') j++;
    // At this point, `j` is either at a LF, EOF, or past EOF.
    if (j > len) invalid_key(key, key_len);
    char* value_raw = raw + i; uint64_t value_raw_len = j - i;

    switch (f.type) {
    case FT_STR:
      if (value_raw_len < f.min || value_raw_len > f.max) invalid_key(key, key_len);
      char* str_val = malloc(value_raw_len + 1);
      memcpy(str_val, value_raw, value_raw_len);
      str_val[value_raw_len] = 0;
      *((char**) (conf_raw + f.offset)) = str_val;
      break;

    case FT_U16:
      if (value_raw_len < 1 || value_raw_len > 5) invalid_key(key, key_len);
      // Use larger size to handle possible overflow.
      uint32_t u16_val = 0;
      for (uint64_t k = 0; k < value_raw_len; k++) {
        if (value_raw[k] < '0' || value_raw[k] > '9') invalid_key(key, key_len);
        u16_val = u16_val * 10 + (value_raw[k] - '0');
      }
      if (u16_val > 65535) invalid_key(key, key_len);
      *((uint16_t*) (conf_raw + f.offset)) = (uint16_t) u16_val;
      break;

    case FT_U64:
      // TODO Handle 20-character values.
      if (value_raw_len < 1 || value_raw_len > 19) invalid_key(key, key_len);
      uint64_t u64_val = 0;
      for (uint64_t k = 0; k < value_raw_len; k++) {
        if (value_raw[k] < '0' || value_raw[k] > '9') invalid_key(key, key_len);
        u64_val = u64_val * 10 + (value_raw[k] - '0');
      }
      *((uint64_t*) (conf + f.offset)) = (uint64_t) u64_val;
      break;

    default:
      fprintf(stderr, "Internal state error: unexpected field type %d\n", f.type);
      exit(EXIT_INTERNAL);
    }

    // At this point, `j` is either at a LF or EOF.
    i = j + 1;
  }
  free(key_buf);

  return conf;
}

void conf_destroy(conf_t* conf) {
  free(conf->device_path);
  free(conf->worker_address);
  free(conf->worker_unix_socket_path);
  free(conf);
}
