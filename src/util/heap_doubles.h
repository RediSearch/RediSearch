/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct double_heap {
    size_t max_size;
    size_t size;
    double data[];
} double_heap_t;

// Create a new double heap with a maximum size (the heap never grows beyond this size)
double_heap_t *double_heap_new(size_t max_size);

/*
 * Add a value to the heap without maintaining the heap property.
 * The heap property can be restored by calling `double_heap_heapify`.
 */
void double_heap_add_raw(double_heap_t *heap, double value);
// Restore the heap property (should be called after adding elements with `double_heap_add_raw`)
void double_heap_heapify(double_heap_t *heap);

// Add a value to the heap and maintain the heap property
void double_heap_push(double_heap_t *heap, double value);
// Remove the top element from the heap
void double_heap_pop(double_heap_t *heap);
// Get the top element from the heap
double double_heap_peek(const double_heap_t *heap);
// Replace the top element with a new value and maintain the heap property
void double_heap_replace(double_heap_t *heap, double value);

// Free the heap
void double_heap_free(double_heap_t *heap);

#ifdef __cplusplus
}
#endif
