#pragma once

#include <stdint.h>

/***

LOSSY MPSC QUEUE
================

A multi-producer single-consumer unbounded queue of pointers with the following caveats:

- It's possible for any entry to be dropped due to capacity, not necessarily in most or least recently added order. When enqueueing, the caller must destruct (if necessary) any entry it may have replaced.
- The least recently added entry might not get dequeued before other entries.

Note that these caveats only apply when the queue is under significant pressure, being close to or at capacity. Under normal operation with a steady enqueue and dequeue flow, the queue maintains FIFO characteristics.

To avoid these caveats, it's recommended for the dequeuer to try dequeue as frequently as possible, even if nothing was dequeued, to prevent bursts of enqueues from causing queue pressure. Also, ensure an appropriate amount of capacity is reserved on creation. The capacity cannot be changed after creation.

Only pointers can be stored so that special empty placeholder values for entries (i.e. NULL) can be used without any extra space.

***/

typedef struct lossy_mpsc_queue_s lossy_mpsc_queue_t;

lossy_mpsc_queue_t* lossy_mpsc_queue_create(uint8_t capacity_log_2);

// Returns an existing entry that needs to be destroyed (if necessary), or NULL.
void* lossy_mpsc_queue_enqueue(lossy_mpsc_queue_t* queue, void* entry);

// May return NULL.
void* lossy_mpsc_queue_dequeue(lossy_mpsc_queue_t* queue);
