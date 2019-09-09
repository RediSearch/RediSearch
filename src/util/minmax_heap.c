#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <stdbool.h>

#include "minmax_heap.h"

#include "rmalloc.h"

#define is_min(n) ((log2_32(n) & 1) == 0)
#define parent(n) (n / 2)
#define first_child(n) (n * 2)
#define second_child(n) ((n * 2) + 1)

#define heap_gt(h, x, y) (h->cmp(h->data[x], h->data[y], h->cmp_ctx) > 0)
#define heap_lt(h, x, y) (h->cmp(h->data[x], h->data[y], h->cmp_ctx) < 0)

#define heap_max(h, x, y) (heap_gt(h, x, y) ? h->data[x] : h->data[y])

static const int tab32[32] = {0, 9,  1,  10, 13, 21, 2,  29, 11, 14, 16, 18, 22, 25, 3, 30,
                              8, 12, 20, 28, 15, 17, 24, 7,  19, 27, 23, 6,  26, 5,  4, 31};

static inline int log2_32(uint32_t value) {
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  return tab32[(uint32_t)(value * 0x07C4ACDD) >> 27];
}

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

int index_max_child_grandchild(heap_t* h, int i) {
  int a = first_child(i);
  int b = second_child(i);
  int d = second_child(a);
  int c = first_child(a);
  int f = second_child(b);
  int e = first_child(b);

  int min_idx = -1;
  if (a <= h->count) min_idx = a;
  if (b <= h->count && heap_gt(h, b, min_idx)) min_idx = b;
  if (c <= h->count && heap_gt(h, c, min_idx)) min_idx = c;
  if (d <= h->count && heap_gt(h, d, min_idx)) min_idx = d;
  if (e <= h->count && heap_gt(h, e, min_idx)) min_idx = e;
  if (f <= h->count && heap_gt(h, f, min_idx)) min_idx = f;

  return min_idx;
}

int index_min_child_grandchild(heap_t* h, int i) {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  int min_idx = -1;
  if (a <= h->count) min_idx = a;
  if (b <= h->count && heap_lt(h, b, min_idx)) min_idx = b;
  if (c <= h->count && heap_lt(h, c, min_idx)) min_idx = c;
  if (d <= h->count && heap_lt(h, d, min_idx)) min_idx = d;
  if (e <= h->count && heap_lt(h, e, min_idx)) min_idx = e;
  if (f <= h->count && heap_lt(h, f, min_idx)) min_idx = f;

  return min_idx;
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

static void trickledown(heap_t* h, int i) {
  if (is_min(i)) {
    trickledown_min(h, i);
  } else {
    trickledown_max(h, i);
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
    trickledown(h, 1);

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
    trickledown(h, idx);

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
