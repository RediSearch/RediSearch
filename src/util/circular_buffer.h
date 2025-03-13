/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

// forward declaration
typedef struct _CircularBuffer _CircularBuffer;
typedef _CircularBuffer* CircularBuffer;

// Creates a new circular buffer, with `cap` items of size `item_size`
CircularBuffer CircularBuffer_New(size_t item_size, uint cap);

// Returns the number of items in the buffer
uint64_t CircularBuffer_ItemCount(CircularBuffer cb);

// Returns buffer capacity.
uint64_t CircularBuffer_Cap(CircularBuffer cb);

// Returns the size of each item in the buffer
uint CircularBuffer_ItemSize(const CircularBuffer cb);

// Returns true if buffer is empty. Thread-safe.
bool CircularBuffer_Empty(const CircularBuffer cb);

// Returns true if buffer is full. Thread-safe.
bool CircularBuffer_Full(const CircularBuffer cb);

// Adds an item to buffer.
// Returns 1 on success, 0 otherwise
// This function is thread-safe and lock-free
int CircularBuffer_Add(CircularBuffer cb, void *item);

// Reserve a slot within buffer.
// Returns a pointer to a 'item size' slot within the buffer.
// This function is thread-safe and lock-free.
// [OUTPUT] wasFull - set to true if buffer is full
void *CircularBuffer_Reserve(CircularBuffer cb, bool *wasFull);

// Read oldest item from buffer.
// This function is not thread-safe.
// This function pops the oldest item from the buffer.
void *CircularBuffer_Read(CircularBuffer cb, void *item);

// Read All items from buffer to dst.
// dst must be large enough to hold all items in the buffer.
// This function is not thread-safe.
// This function copies all items from the buffer to dst.
size_t CircularBuffer_ReadAll(CircularBuffer cb, void *dst, bool advance);

// Sets the read pointer to the beginning of the buffer. Not thread-safe.
void CircularBuffer_ResetReader(CircularBuffer cb);

// Frees buffer (does not free its elements if its free callback is NULL)
void CircularBuffer_Free(CircularBuffer cb);

#ifdef __cplusplus
}
#endif
