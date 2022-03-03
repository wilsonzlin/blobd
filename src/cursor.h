#pragma once

#include <stdint.h>

typedef uint8_t cursor_t;

uint16_t read_u16(cursor_t* cur);

uint32_t read_u24(cursor_t* cur);

uint32_t read_u32(cursor_t* cur);

uint64_t read_u40(cursor_t* cur);

int64_t read_i64(cursor_t* cur);

uint64_t read_u64(cursor_t* cur);

uint16_t consume_u16(cursor_t** cur);

uint32_t consume_u24(cursor_t** cur);

uint32_t consume_u32(cursor_t** cur);

uint64_t consume_u64(cursor_t** cur);

void write_u16(cursor_t* cur, uint16_t v);

void write_u24(cursor_t* cur, uint32_t v);

void write_u32(cursor_t* cur, uint32_t v);

void write_u40(cursor_t* cur, uint64_t v);

void write_u64(cursor_t* cur, uint64_t v);

void produce_u8(cursor_t** cur, uint8_t v);

void produce_u16(cursor_t** cur, uint16_t v);

void produce_u24(cursor_t** cur, uint32_t v);

void produce_u32(cursor_t** cur, uint32_t v);

void produce_u40(cursor_t** cur, uint64_t v);

void produce_u48(cursor_t** cur, uint64_t v);

void produce_u64(cursor_t** cur, uint64_t v);

void produce_n(cursor_t** cur, uint8_t* src, uint64_t n);
