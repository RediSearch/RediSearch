#define _RS_MEMPOOL_C_
#include "mempool.h"
#include <sys/param.h>
typedef struct mempool_t {
  void **entries;
  size_t top;
  size_t cap;
  mempool_alloc_fn alloc;
  mempool_free_fn free;
} mempool_t;

mempool_t *mempool_new(size_t cap, mempool_alloc_fn alloc, mempool_free_fn free) {
  mempool_t *p = malloc(sizeof(mempool_t));
  p->entries = calloc(cap, sizeof(void *));
  p->alloc = alloc;
  p->free = free;
  p->cap = cap;
  p->top = 0;
  return p;
}

void *mempool_get(mempool_t *p) {
  if (p->top > 0) {
    return p->entries[--p->top];
  }
  return p->alloc();
}

void mempool_release(mempool_t *p, void *ptr) {
  if (p->top == p->cap) {
    p->cap += p->cap ? MIN(p->cap, 1024) : 1;
    p->entries = realloc(p->entries, p->cap * sizeof(void *));
  }
  p->entries[p->top++] = ptr;
}

void mempool_destroy(mempool_t *p) {
  for (size_t i = 0; i < p->top; i++) {
    p->free(p->entries[i]);
  }
  free(p->entries);
}
