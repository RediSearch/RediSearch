/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <stddef.h>

typedef struct double_heap {
    size_t max_size;
    size_t size;
    double data[];
} double_heap_t;

double_heap_t *double_heap_new(size_t max_size);

void double_heap_add_raw(double_heap_t *heap, double value);
void double_heap_heapify(double_heap_t *heap);

void double_heap_push(double_heap_t *heap, double value);
void double_heap_pop(double_heap_t *heap);
double double_heap_peek(double_heap_t *heap);
void double_heap_replace(double_heap_t *heap, double value);

void double_heap_free(double_heap_t *heap);
