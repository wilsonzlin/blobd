#pragma once

#include <stddef.h>
#include "cursor.h"

/**

JOURNAL
=======

Only restore from the journal if count and checksum matches.

Structure
---------

u64 xxhash_of_entries_and_count
u32 count
{
  u48 byte_offset
  u32 byte_count
  u8[] bytes_before_change
}[] entries

**/

#define JOURNAL_RESERVED_SPACE (1024 * 1024 * 128)

typedef struct {
  size_t dev_offset;
} journal_t;

journal_t* journal_create(size_t dev_offset);
