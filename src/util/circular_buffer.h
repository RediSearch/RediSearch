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
CircularBuffer CircularBuffer_New
(
	size_t item_size,  // size of item in bytes
	uint cap           // max number of items in buffer
);

// Returns the number of items in the buffer
uint64_t CircularBuffer_ItemCount
(
	CircularBuffer cb  // buffer
);

// Returns buffer capacity
uint64_t CircularBuffer_Cap
(
	CircularBuffer cb // buffer
);

// Returns the size of each item in the buffer
uint CircularBuffer_ItemSize
(
	const CircularBuffer cb  // buffer
);

// Returns true if buffer is empty
bool CircularBuffer_Empty
(
	const CircularBuffer cb  // buffer to inspect
);

// Returns true if buffer is full
bool CircularBuffer_Full
(
	const CircularBuffer cb  // buffer to inspect
);

// Adds an item to buffer.
// Returns 1 on success, 0 otherwise
// This function is thread-safe and lock-free
int CircularBuffer_Add
(
	CircularBuffer cb,  // buffer to populate
	void *item          // item to add
);

// Reserve a slot within buffer.
// Returns a pointer to a 'item size' slot within the buffer.
// This function is thread-safe and lock-free.
void *CircularBuffer_Reserve
(
	CircularBuffer cb  // buffer to populate
);

// Read oldest item from buffer.
// This function is not thread-safe.
// This function pops the oldest item from the buffer.
void *CircularBuffer_Read
(
	CircularBuffer cb,  // buffer to read item from
	void *item          // [optional] pointer populated with removed item
);

// Sets the read pointer to the beginning of the buffer.
void CircularBuffer_ResetReader
(
	CircularBuffer cb  // circular buffer
);

// Frees buffer (does not free its elements if its free callback is NULL)
void CircularBuffer_Free
(
	CircularBuffer cb  // buffer to free
);

#ifdef __cplusplus
}
#endif
