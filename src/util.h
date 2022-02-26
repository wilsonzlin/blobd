#pragma once

size_t min(size_t a, size_t b);

size_t max(size_t a, size_t b);

void min_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u16(uint16_t* a, uint16_t b);

void max_in_place_u32(uint32_t* a, uint32_t b);

size_t uint_divide_ceil(size_t a, size_t b);
