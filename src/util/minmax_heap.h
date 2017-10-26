#ifndef MINMAX_HEAP_H_
#define MINMAX_HEAP_H_

#include <stdlib.h>

typedef int (*mmh_cmp_func)(const void*, const void*, const void*);
typedef void (*mmh_free_func)(void*);
typedef struct heap {

  size_t count;
  size_t size;
  mmh_cmp_func cmp;
  void* cmp_ctx;
  void** data;
  mmh_free_func free_func;
} heap_t;

heap_t* mmh_init(mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func free_func);
heap_t* mmh_init_with_size(size_t size, mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func free_func);
void mmh_free(heap_t* h);

void mmh_dump(heap_t* h);
void mmh_insert(heap_t* h, void* value);
void* mmh_pop_min(heap_t* h);
void* mmh_pop_max(heap_t* h);
void* mmh_peek_min(const heap_t* h);
void* mmh_peek_max(const heap_t* h);

#endif  // MINMAX_HEAP_H_
