#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "device.h"
#include "journal.h"
#include "log.h"
#include "../ext/xxHash/xxhash.h"

LOGGER("journal");

struct journal_s {
  device_t* dev;
  cursor_t* mmap;
  cursor_t* cursor_next;
  uint32_t entry_count;
  uint64_t xxhash_u32_0;
};

journal_t* journal_create(device_t* dev, uint64_t dev_offset) {
  journal_t* journal = malloc(sizeof(journal_t));
  journal->dev = dev;
  journal->mmap = dev->mmap + dev_offset;
  journal->cursor_next = dev->mmap + dev_offset + 8 + 4;
  journal->entry_count = 0;
  return journal;
}

void journal_append(journal_t* jnl, uint64_t dev_offset, uint32_t len) {
  produce_u48(&jnl->cursor_next, dev_offset);
  produce_u32(&jnl->cursor_next, len);
  produce_n(&jnl->cursor_next, jnl->dev->mmap + dev_offset, len);
}

void journal_flush(journal_t* jnl) {
  write_u32(jnl->mmap + 8, jnl->entry_count);
  uint64_t journal_checksum = XXH3_64bits(jnl->mmap + 8, jnl->cursor_next - (jnl->mmap + 8));
  write_u64(jnl->mmap, journal_checksum);
  device_sync(jnl->dev);
}

void journal_clear(journal_t* jnl) {
  jnl->cursor_next = jnl->mmap + 8 + 4;
  jnl->entry_count = 0;
  write_u64(jnl->mmap, jnl->xxhash_u32_0);
  write_u32(jnl->mmap + 4, 0);
  // We must flush, or else we'll try recovering from journal and overwrite data.
  device_sync(jnl->dev);
}

typedef struct {
  uint64_t dev_offset;
  uint32_t len;
  cursor_t* data;
} change_t;

void journal_apply_or_clear(journal_t* jnl) {
  uint64_t checksum_recorded = read_u64(jnl->mmap);
  cursor_t* start = jnl->mmap + 8;
  cursor_t* cur = start;
  uint32_t count = consume_u32(&cur);
  // We can't apply the changes until we verify the checksum is accurate, but we cannot verify the checksum until we get the byte length which requires reading them.
  change_t* changes = malloc(sizeof(change_t) * count);
  for (uint32_t i = 0; i < count; i++) {
    // TODO Assert cursor doesn't go past JOURNAL_RESERVED_SPACE.
    uint64_t dev_offset = consume_u48(&cur);
    uint32_t len = consume_u32(&cur);
    cursor_t* data = cur;
    change_t c = {
      .data = data,
      .dev_offset = dev_offset,
      .len = len,
    };
    changes[i] = c;
  }
  uint64_t checksum_actual = XXH3_64bits(start, cur - start);
  if (checksum_recorded != checksum_actual) {
    // Just because the checksum doesn't match doesn't mean there's corruption:
    // - The process might've crashed while writing the journal.
    // - The process might've crashed while clearing the journal.
    ts_log(WARN, "Journal is not intact and will not be applied");
    // For extra safety, don't clear journal in case we want to look at it.
  } else if (count) {
    ts_log(WARN, "Intact journal found, applying");
    for (uint32_t i = 0; i < count; i++) {
      change_t c = changes[i];
      memcpy(jnl->dev->mmap + c.dev_offset, c.data, c.len);
    }
    // Ensure sync BEFORE clearing journal.
    device_sync(jnl->dev);
    ts_log(INFO, "Journal applied");
    journal_clear(jnl);
    ts_log(INFO, "Journal cleared");
  }
  free(changes);
}
