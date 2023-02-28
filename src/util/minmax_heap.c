/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <stdbool.h>

#include "minmax_heap.h"

#include "rmalloc.h"

// parity of the number of leading zeros, AKA parity of log2(n)
#define is_min(n) (__builtin_clz(n) & 1)

#define parent(n) (n / 2)
#define first_child(n) (n * 2)
#define second_child(n) ((n * 2) + 1)

#define heap_gt(h, x, y) (h->cmp(h->data[x], h->data[y], h->cmp_ctx) > 0)
#define heap_lt(h, x, y) (h->cmp(h->data[x], h->data[y], h->cmp_ctx) < 0)

#define heap_max(h, x, y) (heap_gt(h, x, y) ? h->data[x] : h->data[y])

#define choose_from_3(fn, a, b, c) \
  (fn(h, a, b) ? (fn(h, a, c) ? a : c) : (fn(h, b, c) ? b : c))

#define choose_from_4(fn, a, b, c, d) \
  (fn(h, a, b) ? choose_from_3(fn, a, c, d) : choose_from_3(fn, b, c, d))

#define swap(h, i, j)        \
  {                          \
    void* tmp = h->data[i];  \
    h->data[i] = h->data[j]; \
    h->data[j] = tmp;        \
  }

static void bubbleup_min(heap_t* h, int i) {
  int pp_idx = parent(parent(i));
  if (pp_idx <= 0) return;

  if (heap_lt(h, i, pp_idx)) {
    swap(h, i, pp_idx);
    bubbleup_min(h, pp_idx);
  }
}

static void bubbleup_max(heap_t* h, int i) {
  int pp_idx = parent(parent(i));
  if (pp_idx <= 0) return;

  if (heap_gt(h, i, pp_idx)) {
    swap(h, i, pp_idx);
    bubbleup_max(h, pp_idx);
  }
}

static void bubbleup(heap_t* h, int i) {
  int p_idx = parent(i);
  if (p_idx <= 0) return;

  if (is_min(i)) {
    if (heap_gt(h, i, p_idx)) {
      swap(h, i, p_idx);
      bubbleup_max(h, p_idx);
    } else {
      bubbleup_min(h, i);
    }
  } else {
    if (heap_lt(h, i, p_idx)) {
      swap(h, i, p_idx);
      bubbleup_min(h, p_idx);
    } else {
      bubbleup_max(h, i);
    }
  }
}

static inline int highest_index(heap_t* h, int i) {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  if (f <= h->count) return 5;
  if (e <= h->count) return 4;
  if (d <= h->count) return 3;
  if (c <= h->count) return 2;
  if (b <= h->count) return 1;
  if (a <= h->count) return 0;
  return -1;
}

int index_max_child_grandchild(heap_t* h, int i) {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  switch (highest_index(h, i)) {
    case 5:
      return choose_from_4(heap_gt, c, d, e, f);
    case 4:
      return choose_from_3(heap_gt, c, d, e);
    case 3:
      return choose_from_3(heap_gt, b, c, d);
    case 2:
      return heap_gt(h, b, c) ? b : c;
    case 1:
      return heap_gt(h, a, b) ? a : b;
    case 0:
      return a;
    default:
      return -1;
  }
}

int index_min_child_grandchild(heap_t* h, int i) {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  switch (highest_index(h, i)) {
    case 5:
      return choose_from_4(heap_lt, c, d, e, f);
    case 4:
      return choose_from_3(heap_lt, c, d, e);
    case 3:
      return choose_from_3(heap_lt, b, c, d);
    case 2:
      return heap_lt(h, b, c) ? b : c;
    case 1:
      return heap_lt(h, a, b) ? a : b;
    case 0:
      return a;
    default:
      return -1;
  }
}

static void trickledown_max(heap_t* h, int i) {
  int m = index_max_child_grandchild(h, i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (heap_gt(h, m, i)) {
      swap(h, i, m);
      if (heap_lt(h, m, parent(m))) {
        swap(h, m, parent(m));
      }
      trickledown_max(h, m);
    }
  } else {
    // m is a child
    if (heap_gt(h, m, i)) swap(h, i, m);
  }
}

static void trickledown_min(heap_t* h, int i) {
  int m = index_min_child_grandchild(h, i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (heap_lt(h, m, i)) {
      swap(h, i, m);
      if (heap_gt(h, m, parent(m))) {
        swap(h, m, parent(m));
      }
      trickledown_min(h, m);
    }
  } else {
    // m is a child
    if (heap_lt(h, m, i)) swap(h, i, m);
  }
}

void mmh_insert(heap_t* h, void* value) {
  assert(value != NULL);
  h->count++;
  // check for realloc
  if (h->count == h->size) {
    h->size = h->size * 2;
    h->data = rm_realloc(h->data, (1 + h->size) * sizeof(void*));
  }
  h->data[h->count] = value;
  bubbleup(h, h->count);
}

void* mmh_pop_min(heap_t* h) {
  if (h->count > 1) {
    void* d = h->data[1];
    h->data[1] = h->data[h->count--];
    trickledown_min(h, 1);

    return d;
  }

  if (h->count == 1) {
    h->count--;
    return h->data[1];
  }
  return NULL;
}

void* mmh_pop_max(heap_t* h) {
  if (h->count > 2) {
    int idx = 2;
    if (heap_lt(h, 2, 3)) idx = 3;
    void* d = h->data[idx];
    h->data[idx] = h->data[h->count--];
    trickledown_max(h, idx);

    return d;
  }

  if (h->count == 2) {
    h->count--;
    return h->data[2];
  }

  if (h->count == 1) {
    h->count--;
    return h->data[1];
  }
  return NULL;
}

void* mmh_peek_min(const heap_t* h) {
  if (h->count > 0) {
    return h->data[1];
  }
  return NULL;
}

void* mmh_peek_max(const heap_t* h) {
  if (h->count > 2) {
    return heap_max(h, 2, 3);  // h->data[2], h->data[3]);
  }
  if (h->count == 2) {
    return h->data[2];
  }
  if (h->count == 1) {
    return h->data[1];
  }
  return NULL;
}

// void mmh_dump(heap_t* h) {
//   printf("count is %d, elements are:\n\t [", h->count);
//   for (int i = 1; i <= h->count; i++) {
//     printf(" %d ", h->data[i]);
//   }
//   printf("]\n");
// }

heap_t* mmh_init(mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func ff) {
  return mmh_init_with_size(50, cmp, cmp_ctx, ff);
}

heap_t* mmh_init_with_size(size_t size, mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func ff) {
  // first array element is wasted since 1st heap element is on position 1
  // inside the array i.e. => [0,(1),(2), ... (n)] so minimum viable size is 2
  size = size > 2 ? size : 2;
  heap_t* h = rm_calloc(1, sizeof(heap_t));
  // We allocate 1 extra space because we start at index 1
  h->data = rm_calloc(size + 1, sizeof(void*));
  h->count = 0;
  h->size = size;
  h->cmp = cmp;
  h->cmp_ctx = cmp_ctx;
  h->free_func = ff;
  return h;
}

void mmh_free(heap_t* h) {
  if (h->free_func) {
    for (size_t i = 0; i <= h->count; i++) {
      h->free_func(h->data[i]);
    }
  }
  rm_free(h->data);
  rm_free(h);
}
