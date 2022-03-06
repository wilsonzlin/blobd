#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include "lossy_mpsc_queue.h"

struct lossy_mpsc_queue_s {
  uint64_t capacity_mask;
  uint64_t read_next;
  _Atomic(uint64_t) write_next;
  _Atomic(void*) entries[];
};

lossy_mpsc_queue_t* lossy_mpsc_queue_create(uint8_t capacity_log_2) {
  uint64_t cap = 1llu << capacity_log_2;
  // We cannot use calloc or memset as NULL is not guaranteed to be zero.
  lossy_mpsc_queue_t* queue = malloc(sizeof(lossy_mpsc_queue_t*) + cap * sizeof(void*));
  // We can't use `(1 << capacity_log_2) - 1` as `capacity_log_2` could be 64.
  queue->capacity_mask = 0xFFFFFFFFFFFFFFFF >> (64 - capacity_log_2);
  queue->read_next = 0;
  atomic_init(&queue->write_next, 0);
  for (uint64_t i = 0; i < cap; i++) {
    atomic_init(&queue->entries[i], NULL);
  }
  return queue;
}

// Returns an existing entry that needs to be destroyed (if necessary), or NULL.
void* lossy_mpsc_queue_enqueue(lossy_mpsc_queue_t* queue, void* entry) {
  uint64_t idx = atomic_load_explicit(&queue->write_next, memory_order_relaxed);
  while (!atomic_compare_exchange_weak_explicit(&queue->write_next, &idx, (idx + 1) & queue->capacity_mask, memory_order_relaxed, memory_order_relaxed)) {};
  void* existing = atomic_exchange_explicit(queue->entries + idx, entry, memory_order_relaxed);
  return existing;
}

// May return NULL.
void* lossy_mpsc_queue_dequeue(lossy_mpsc_queue_t* queue) {
  uint64_t idx = queue->read_next;
  void* entry = atomic_exchange_explicit(queue->entries + idx, NULL, memory_order_relaxed);
  if (entry != NULL) {
    queue->read_next = (queue->read_next + 1) & queue->capacity_mask;
  }
  return entry;
}
