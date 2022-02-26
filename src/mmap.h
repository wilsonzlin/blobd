#pragma once

#include <unistd.h>

// This must be initialised by main() before calling any function.
size_t MMAP_PAGE_SIZE;

#define ALIGN(cur) ((cur) & ~(MMAP_PAGE_SIZE - 1))
