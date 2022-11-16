/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include "assert.h"
#include "priority_queue.h"
#include "redismodule.h"
#include "alloc.h"

int cmp(void *i1, void *i2) {
  int *__i1 = (int *)i1;
  int *__i2 = (int *)i2;
  return *__i1 - *__i2;
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  PriorityQueue *pq = NewPriorityQueue(int, 10, cmp);
  assert(0 == Priority_Queue_Size(pq));

  for (int i = 0; i < 5; i++) {
    Priority_Queue_Push(pq, i);
  }
  assert(5 == Priority_Queue_Size(pq));

  Priority_Queue_Pop(pq);
  assert(4 == Priority_Queue_Size(pq));

  Priority_Queue_Push(pq, 10);
  Priority_Queue_Push(pq, 20);
  Priority_Queue_Push(pq, 15);
  int n;
  Priority_Queue_Top(pq, &n);
  assert(20 == n);

  Priority_Queue_Pop(pq);
  Priority_Queue_Top(pq, &n);
  assert(15 == n);

  Priority_Queue_Free(pq);
  printf("PASS!\n");
  return 0;
}
