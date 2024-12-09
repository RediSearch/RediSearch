/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "heap_doubles.h"
#include "rmalloc.h"
#include <assert.h>

static inline int child_left(const int idx) {
  return idx * 2 + 1;
}

static inline int child_right(const int idx) {
  return idx * 2 + 2;
}

static inline int parent(const int idx) {
  return (idx - 1) / 2;
}

static inline void swap(double_heap_t *h, const int i1, const int i2) {
  double tmp = h->data[i1];
  h->data[i1] = h->data[i2];
  h->data[i2] = tmp;
}

static void push_up(double_heap_t *h, unsigned int idx) {
  /* 0 is the root node */
  while (0 != idx) {
    int parent_idx = parent(idx);

    /* we are smaller than the parent */
    if (h->data[idx] < h->data[parent_idx])
      return;
    else
      swap(h, idx, parent_idx);

    idx = parent_idx;
  }
}

static void push_down(double_heap_t *h, unsigned int idx) {
  while (1) {
    unsigned int l, r, child;

    l = child_left(idx);
    r = child_right(idx);

    if (r >= h->size) {
      /* can't push_down any further */
      if (l >= h->size) return;
      child = l;

    } else {
      /* find biggest child */
      child = (h->data[l] > h->data[r]) ? l : r;
    }

    /* idx is smaller than child */
    if (h->data[idx] < h->data[child]) {
      swap(h, idx, child);
      idx = child;
    } else {
      return; /* bigger than the biggest child, we stop, we win */
    }
  }
}

/****************************************** API ******************************************/

double_heap_t *double_heap_new(size_t max_size) {
  double_heap_t *heap = rm_malloc(sizeof(*heap) + max_size * sizeof(double));
  heap->max_size = max_size;
  heap->size = 0;
  return heap;
}

void double_heap_add_raw(double_heap_t *heap, double value) {
  assert(heap->size < heap->max_size);
  heap->data[heap->size++] = value;
}

void double_heap_heapify(double_heap_t *heap) {
  for (int i = heap->size / 2; i >= 0; i--) {
    push_down(heap, i);
  }
}

void double_heap_push(double_heap_t *heap, double value) {
  assert(heap->size < heap->max_size);
  heap->data[heap->size] = value;
  push_up(heap, heap->size);
  heap->size++;
}

double double_heap_peek(const double_heap_t *heap) {
  assert(heap->size > 0);
  return heap->data[0];
}

void double_heap_pop(double_heap_t *heap) {
  assert(heap->size > 0);
  heap->size--;
  heap->data[0] = heap->data[heap->size];
  push_down(heap, 0);
}

void double_heap_replace(double_heap_t *heap, double value) {
  heap->data[0] = value;
  push_down(heap, 0);
}

void double_heap_free(double_heap_t *heap) {
  rm_free(heap);
}
