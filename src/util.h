#pragma once

uint64_t min(uint64_t a, uint64_t b);

uint64_t max(uint64_t a, uint64_t b);

void min_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u32(uint32_t* a, uint32_t b);

uint64_t uint_divide_ceil(uint64_t a, uint64_t b);

int maybe_read(int fd, uint8_t* out_buf, uint64_t n);

int maybe_write(int fd, uint8_t* in_buf, uint64_t n);
