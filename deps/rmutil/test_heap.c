/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include "heap.h"
#include "assert.h"
#include "redismodule.h"
#include "alloc.h"

int cmp(void *a, void *b) {
  int *__a = (int *)a;
  int *__b = (int *)b;
  return *__a - *__b;
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  int myints[] = {10, 20, 30, 5, 15};
  Vector *v = NewVector(int, 5);
  for (int i = 0; i < 5; i++) {
    Vector_Push(v, myints[i]);
  }

  Make_Heap(v, 0, v->top, cmp);

  int n;
  Vector_Get(v, 0, &n);
  assert(30 == n);

  Heap_Pop(v, 0, v->top, cmp);
  v->top = 4;
  Vector_Get(v, 0, &n);
  assert(20 == n);

  Vector_Push(v, 99);
  Heap_Push(v, 0, v->top, cmp);
  Vector_Get(v, 0, &n);
  assert(99 == n);

  Vector_Free(v);
  printf("PASS!\n");
  return 0;
}
