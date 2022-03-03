#pragma once

#include <stdint.h>
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

typedef struct journal_s journal_t;

journal_t* journal_create(device_t* dev, uint64_t dev_offset);

void journal_append(journal_t* jnl, uint64_t dev_offset, uint32_t len);

void journal_flush(journal_t* jnl);

void journal_clear(journal_t* jnl);
