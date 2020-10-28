#include <stdio.h>
#include <assert.h>
#include "../../src/dep/skiplist/skiplist.h"
#include "rmutil/alloc.h"

void RMUTil_InitAlloc();

int cmp(void *a, void *b) {
  double da = *(double *)a;
  double db = *(double *)b;
  return da > db ? 1 : da < db ? -1 : 0;
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  skiplist *sl = slCreate(&cmp, NULL);
  double num[5] = {1, 42, -20000, 0, 10};
  for (int i = 0; i < 5; ++i) {
    slInsert(sl, &num[i]);
  }
  assert(slGetRank(sl, &num[2]) == 1);  // -20000
  assert(slGetRank(sl, &num[3]) == 2);  // 0 
  assert(slGetRank(sl, &num[0]) == 3);  // 1
  assert(slGetRank(sl, &num[4]) == 4);  // 10
  assert(slGetRank(sl, &num[1]) == 5);  // 42
  
  assert(slGetLength(sl) == 5);

  double in = 0;
  double *out = NULL;
  assert(slDelete(sl, (void *)&num[3], (void **)&out) == 1);

  assert(slGetRank(sl, &num[2]) == 1);  // -20000
  assert(slGetRank(sl, &num[0]) == 2);  // 1
  assert(slGetRank(sl, &num[4]) == 3);  // 10
  assert(slGetRank(sl, &num[1]) == 4);  // 42
  assert(out == &num[3]);               // 0 (deleted)

  double start = -100, end = 27;
  skiplistIterator *iter = slIteratorCreate(sl, &start, &end);
  assert(slIteratorNext(iter) == &num[0]);    // 1
  assert(slIteratorNext(iter) == &num[4]);    // 10
  assert(slIteratorNext(iter) == NULL);
  slIteratorDestroy(iter);

  start = -100000;
  end = 5;
  iter = slIteratorCreate(sl, &start, &end);
  assert(slIteratorNext(iter) == &num[2]);    // -20000
  assert(slIteratorNext(iter) == &num[0]);    // 1
  assert(slIteratorNext(iter) == NULL);
  slIteratorDestroy(iter);

  start = 5;
  end = 100000;
  iter = slIteratorCreate(sl, &start, &end);
  assert(slIteratorNext(iter) == &num[4]);    // 10
  assert(slIteratorNext(iter) == &num[1]);    // 42
  assert(slIteratorNext(iter) == NULL);
  slIteratorDestroy(iter);

  start = -100000;
  end = 100000;
  iter = slIteratorCreate(sl, &start, &end);
  assert(slIteratorNext(iter) == &num[2]);    // -20000
  assert(slIteratorNext(iter) == &num[0]);    // 1
  assert(slIteratorNext(iter) == &num[4]);    // 10
  assert(slIteratorNext(iter) == &num[1]);    // 42
  assert(slIteratorNext(iter) == NULL);
  slIteratorDestroy(iter);

  // tests for single value
  assert(slDelete(sl, (void *)&num[1], (void **)&out) == 1);  // 42
  assert(slDelete(sl, (void *)&num[2], (void **)&out) == 1);  // -20000
  assert(slDelete(sl, (void *)&num[4], (void **)&out) == 1);  // 10

  start = -10;
  end = -8;
  iter = slIteratorCreate(sl, &start, &end);
  assert(iter == NULL);

  start = 8;
  end = 10;
  iter = slIteratorCreate(sl, &start, &end);
  assert(iter == NULL);
  
  start = 0;
  end = 10;
  iter = slIteratorCreate(sl, &start, &end);
  assert(slIteratorNext(iter) == &num[0]);    // 1
  assert(slIteratorNext(iter) == NULL);
  slIteratorDestroy(iter);

  slFree(sl);

  return 0;
}