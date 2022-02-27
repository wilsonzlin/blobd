#define _DEFAULT_SOURCE

#include <endian.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "cursor.h"

uint16_t read_u16(cursor_t* cur) {
  uint16_t v;
  memcpy(&v, cur, 2);
  return be16toh(v);
}

uint32_t read_u24(cursor_t* cur) {
  uint32_t v = cur[0];
  v = (v << 8) | cur[1];
  v = (v << 8) | cur[2];
  return v;
}

uint32_t read_u32(cursor_t* cur) {
  uint32_t v;
  memcpy(&v, cur, 4);
  return be32toh(v);
}

uint64_t read_u40(cursor_t* cur) {
  uint64_t v = cur[0];
  v = (v << 8) | cur[1];
  v = (v << 8) | cur[2];
  v = (v << 8) | cur[3];
  v = (v << 8) | cur[4];
  return v;
}

uint64_t read_u64(cursor_t* cur) {
  uint64_t v;
  memcpy(&v, cur, 8);
  return be64toh(v);
}

uint16_t consume_u16(cursor_t** cur) {
  uint16_t v = read_u16(*cur);
  *cur += 2;
  return v;
}

uint32_t consume_u24(cursor_t** cur) {
  uint32_t v = read_u24(*cur);
  *cur += 3;
  return v;
}

uint32_t consume_u32(cursor_t** cur) {
  uint32_t v = read_u32(*cur);
  *cur += 4;
  return v;
}

uint64_t consume_u64(cursor_t** cur) {
  uint64_t v = read_u64(*cur);
  *cur += 8;
  return v;
}

void write_u16(cursor_t* cur, uint16_t v) {
  uint16_t encoded = htobe16(v);
  memcpy(cur, &encoded, 2);
}

void write_u24(cursor_t* cur, uint32_t v) {
  cur[2] = v & 0xff; v >>= 8;
  cur[1] = v & 0xff; v >>= 8;
  cur[0] = v & 0xff;
}

void write_u32(cursor_t* cur, uint32_t v) {
  uint32_t encoded = htobe32(v);
  memcpy(cur, &encoded, 4);
}

void write_u40(cursor_t* cur, uint64_t v) {
  cur[4] = v & 0xff; v >>= 8;
  cur[3] = v & 0xff; v >>= 8;
  cur[2] = v & 0xff; v >>= 8;
  cur[1] = v & 0xff; v >>= 8;
  cur[0] = v & 0xff;
}

void write_u48(cursor_t* cur, uint64_t v) {
  cur[5] = v & 0xff; v >>= 8;
  cur[4] = v & 0xff; v >>= 8;
  cur[3] = v & 0xff; v >>= 8;
  cur[2] = v & 0xff; v >>= 8;
  cur[1] = v & 0xff; v >>= 8;
  cur[0] = v & 0xff;
}

void write_u64(cursor_t* cur, uint64_t v) {
  uint64_t encoded = htobe64(v);
  memcpy(cur, &encoded, 8);
}

void produce_u8(cursor_t** cur, uint8_t v) {
  **cur = v;
  *cur += 1;
}

void produce_u16(cursor_t** cur, uint16_t v) {
  write_u16(*cur, v);
  *cur += 2;
}

void produce_u24(cursor_t** cur, uint32_t v) {
  write_u24(*cur, v);
  *cur += 3;
}

void produce_u32(cursor_t** cur, uint32_t v) {
  write_u32(*cur, v);
  *cur += 4;
}

void produce_u40(cursor_t** cur, uint64_t v) {
  write_u40(*cur, v);
  *cur += 5;
}

void produce_u48(cursor_t** cur, uint64_t v) {
  write_u48(*cur, v);
  *cur += 6;
}

void produce_u64(cursor_t** cur, uint64_t v) {
  write_u64(*cur, v);
  *cur += 8;
}

void produce_n(cursor_t** cur, uint8_t* src, size_t n) {
  memcpy(*cur, src, n);
  *cur += n;
}
