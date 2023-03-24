/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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
} mm_heap_t;

mm_heap_t* mmh_init(mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func free_func);
mm_heap_t* mmh_init_with_size(size_t size, mmh_cmp_func cmp, void* cmp_ctx, mmh_free_func free_func);
void mmh_free(mm_heap_t* h);
void mmh_clear(mm_heap_t* h);

// void mmh_dump(mm_heap_t* h);
void mmh_insert(mm_heap_t* h, void* value);
void* mmh_pop_min(mm_heap_t* h);
void* mmh_pop_max(mm_heap_t* h);
void* mmh_peek_min(const mm_heap_t* h);
void* mmh_peek_max(const mm_heap_t* h);
void* mmh_exchange_min(mm_heap_t* h, void* value); // combines pop-and-then-insert logic
void* mmh_exchange_max(mm_heap_t* h, void* value); // combines pop-and-then-insert logic

#endif  // MINMAX_HEAP_H_
