/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redismodule.h"
#include "circular_buffer.h"
#include "src/rmalloc.h"
#include "src/util/likely.h"

#include <stdatomic.h>

// Circular buffer structure.
// The buffer is of fixed size.
// Items are removed by order of insertion, similar to a queue.
struct _CircularBuffer {
  char *read;                   // read data from here
  _Atomic uint64_t write;       // write offset into data
  size_t item_size;             // item size in bytes
  _Atomic uint64_t item_count;  // current number of items in buffer
  uint64_t item_cap;            // max number of items held by buffer
  char *end_marker;             // marks the end of the buffer
  char data[];                  // data
};

// Creates a new circular buffer, with `cap` items of size `item_size`
CircularBuffer CircularBuffer_New(size_t item_size, uint cap) {
  CircularBuffer cb = rm_calloc(1, sizeof(_CircularBuffer) + item_size * cap);

  cb->read       = cb->data;                      // initial read position
  cb->write      = ATOMIC_VAR_INIT(0);            // write offset into data
  cb->item_cap   = cap;                           // buffer capacity
  cb->item_size  = item_size;                     // item size
  cb->item_count = ATOMIC_VAR_INIT(0);            // no items in buffer
  cb->end_marker = cb->data + (item_size * cap);  // end of data marker

  return cb;
}

// Returns the number of items in the buffer. Thread-safe.
uint64_t CircularBuffer_ItemCount(CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  return atomic_load(&cb->item_count);
}

// Returns buffer capacity.
uint64_t CircularBuffer_Cap(CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  return cb->item_cap;
}

// Returns the size of each item in the buffer.
uint CircularBuffer_ItemSize(const CircularBuffer cb) {
  return cb->item_size;
}

// Returns true if buffer is empty. Thread-safe.
inline bool CircularBuffer_Empty(const CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  uint64_t item_count = atomic_load(&cb->item_count);
  return item_count == 0;
}

// Returns true if buffer is full. Thread-safe.
inline bool CircularBuffer_Full(const CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  uint64_t item_count = atomic_load(&cb->item_count);
  return item_count == cb->item_cap;
}

// Adds an item to buffer.
// Returns 1 on success, 0 otherwise
// This function is thread-safe and lock-free
int CircularBuffer_Add(CircularBuffer cb, void *item) {
  RedisModule_Assert(cb   != NULL);
  RedisModule_Assert(item != NULL);

  // atomic update buffer item count
  // do not add item if buffer is full
  uint64_t item_count = atomic_fetch_add(&cb->item_count, 1);
  if (unlikely(item_count >= cb->item_cap)) {
    atomic_fetch_sub(&cb->item_count, 1);
    return 0;
  }

  // determine current and next write position
  uint64_t offset = atomic_fetch_add(&cb->write, cb->item_size);

  // check for buffer overflow
  if (unlikely(cb->data + offset >= cb->end_marker)) {
    // write need to circle back
    // [., ., ., ., ., ., A, B, C]
    //                           ^  ^
    //                           W0 W1
    uint64_t overflow = offset + cb->item_size;

    // adjust offset
    // [., ., ., ., ., ., A, B, C]
    //  ^  ^
    //  W0 W1
    offset -= cb->item_size * cb->item_cap;

    // update write position
    // multiple threads "competing" to update write position
    // we ensure that the thread with the largest offset will succeed
    // for the above example, W1 will succeed
    //
    // [., ., ., ., ., ., A, B, C]
    //        ^
    //        W
    atomic_compare_exchange_strong(&cb->write, &overflow, offset + cb->item_size);
  }

  // copy item into buffer
  memcpy(cb->data + offset, item, cb->item_size);

  // report success
  return 1;
}

// Reserve a slot within buffer.
// Returns a pointer to a 'item size' slot within the buffer.
// This function is thread-safe and lock-free.
void *CircularBuffer_Reserve(CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  // atomic update buffer item count
  // an item will be overwritten if buffer is full
  uint64_t item_count = atomic_fetch_add(&cb->item_count, 1);
  if (unlikely(item_count >= cb->item_cap)) {
    atomic_fetch_sub(&cb->item_count, 1);
  }

  // determine current and next write position
  uint64_t offset = atomic_fetch_add(&cb->write, cb->item_size);

  // check for buffer overflow
  if (unlikely(cb->data + offset >= cb->end_marker)) {
    // write need to circle back
    // [., ., ., ., ., ., A, B, C]
    //                           ^  ^
    //                           W0 W1
    uint64_t overflow = offset + cb->item_size;

    // adjust offset
    // [., ., ., ., ., ., A, B, C]
    //  ^  ^
    //  W0 W1
    offset -= cb->item_size * cb->item_cap;

    // update write position
    // multiple threads "competing" to update write position
    // only the thread with the largest offset will succeed
    // for the above example, W1 will succeed
    //
    // [., ., ., ., ., ., A, B, C]
    //        ^
    //        W
    atomic_compare_exchange_strong(&cb->write, &overflow, offset + cb->item_size);
  }

  // return slot pointer
  return cb->data + offset;
}

// Read oldest item from buffer.
// This function is not thread-safe.
// This function pops the oldest item from the buffer.
void *CircularBuffer_Read(CircularBuffer cb, void *item) {
  RedisModule_Assert(cb != NULL);

  // make sure there's data to return
  if (unlikely(CircularBuffer_Empty(cb))) {
    return NULL;
  }

  void *read = cb->read;

  // update buffer item count
  cb->item_count--;

  // copy item from buffer to output
  if (item != NULL) {
    memcpy(item, cb->read, cb->item_size);
  }

  // advance read position
  // circle back if read reached the end of the buffer
  cb->read += cb->item_size;
  if (unlikely(cb->read >= cb->end_marker)) {
    cb->read = cb->data;
  }

  // return original read position
  return read;
}

// Sets the read pointer to the beginning of the buffer. Not thread-safe.
// assuming the buffer looks like this:
//
// [., ., ., A, B, C, ., ., .]
//                    ^
//                    W
//
// CircularBuffer_ResetReader will set 'read' to A
//
// [., ., ., A, B, C, ., ., .]
//           ^        ^
//           R        W
//
void CircularBuffer_ResetReader(CircularBuffer cb) {
  // compensate for circularity
  uint64_t write = cb->write;

  // compute newest item index, e.g. newest item is at index k
  uint idx = write / cb->item_size;

  // compute offset to oldest item
  // oldest item is n elements before newest item
  //
  // example:
  //
  // [C, ., ., ., ., ., ., A, B]
  //
  // idx = 1, item_count = 3
  // offset is 1 - 3 = -2
  //
  // [C, ., ., ., ., ., ., A, B]
  //     ^                 ^
  //     W                 R

  int offset = idx - cb->item_count;
  offset *= cb->item_size;

  if (offset >= 0) {
    // offset is positive, read from beginning of buffer
    cb->read = cb->data + offset;
  } else {
    // offset is negative, read from end of buffer
    cb->read = cb->end_marker + offset;
  }
}

// Frees buffer (does not free its elements if its free callback is NULL)
void CircularBuffer_Free(CircularBuffer cb) {
  RedisModule_Assert(cb != NULL);

  rm_free(cb);
}
