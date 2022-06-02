#include <stdio.h>
#include "assert.h"
#include "priority_queue.h"
#include "redismodule.h"
#include "alloc.h"
REDISMODULE_INIT_SYMBOLS();

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  PriorityQueue pq = new PriorityQueue<int>(10);
  assert(0 == pq.Size());

  for (int i = 0; i < 5; i++) {
    pq.Push(i);
  }
  assert(5 == pq.Size());

  pq.Pop();
  assert(4 == pq.Size());

  pq.Push(10);
  pq.Push(20);
  pq.Push(15);
  int n;
  pq.Top(&n);
  assert(20 == n);

  pq.Pop();
  pq.Top(&n);
  assert(15 == n);

  printf("PASS!\n");
  return 0;
}
