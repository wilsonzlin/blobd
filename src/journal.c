#include <stddef.h>
#include <stdlib.h>
#include "journal.h"

journal_t* journal_create(size_t dev_offset) {
  journal_t* journal = malloc(sizeof(journal_t));
  journal->dev_offset = dev_offset;
  return journal;
}
