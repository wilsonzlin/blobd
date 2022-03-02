#include <stddef.h>
#include <stdlib.h>
#include <sys/mman.h>
#include "device.h"
#include "journal.h"
#include "../ext/xxHash/xxhash.h"

struct journal_s {
  device_t* dev;
  cursor_t* mmap;
  cursor_t* cursor_next;
  uint32_t entry_count;
  uint64_t xxhash_u32_0;
};

journal_t* journal_create(device_t* dev, size_t dev_offset) {
  journal_t* journal = malloc(sizeof(journal_t));
  journal->dev = dev;
  journal->mmap = dev->mmap + dev_offset;
  journal->cursor_next = dev->mmap + dev_offset + 8 + 4;
  journal->entry_count = 0;
  return journal;
}

void journal_append(journal_t* jnl, size_t dev_offset, uint32_t len) {
  produce_u48(&jnl->cursor_next, dev_offset);
  produce_u32(&jnl->cursor_next, len);
  produce_n(&jnl->cursor_next, jnl->dev->mmap + dev_offset, len);
}

void journal_flush(journal_t* jnl) {
  write_u32(jnl->mmap + 8, jnl->entry_count);
  uint64_t journal_checksum = XXH3_64bits(jnl->mmap + 8, jnl->cursor_next - (jnl->mmap + 8));
  write_u64(jnl->mmap, journal_checksum);
  msync(jnl->dev->mmap, jnl->dev->size, MS_SYNC);
}

void journal_clear(journal_t* jnl) {
  jnl->cursor_next = jnl->mmap + 12;
  jnl->entry_count = 0;
  write_u64(jnl->mmap, jnl->xxhash_u32_0);
  write_u32(jnl->mmap + 4, 0);
  // We must flush, or else we'll try recovering from journal and overwrite data.
  msync(jnl->dev->mmap, jnl->dev->size, MS_SYNC);
}
