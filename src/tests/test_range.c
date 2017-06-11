#include "../numeric_index.h"
#include <stdio.h>
#include "test_util.h"
#include "time_sample.h"
#include "../index.h"
#include "../rmutil/alloc.h"

// Helper so we get the same pseudo-random numbers
// in tests across environments
u_int prng_seed = 1337;
#define PRNG_MOD 30980347
u_int prng() {
  prng_seed = (prng_seed * prng_seed) % PRNG_MOD;
  return prng_seed;
}

int testNumericRangeTree() {
  NumericRangeTree *t = NewNumericRangeTree();
  ASSERT(t != NULL);

  for (int i = 0; i < 50000; i++) {

    NumericRangeTree_Add(t, i + 1, (double)(1 + prng() % 5000));
  }
  ASSERT_EQUAL(t->numRanges, 16);
  ASSERT_EQUAL(t->numEntries, 50000);

  struct {
    double min;
    double max;
  } rngs[] = {{0, 100}, {10, 1000}, {2500, 3500}, {0, 5000}, {4999, 4999}, {0, 0}};

  for (int r = 0; rngs[r].min || rngs[r].max; r++) {

    Vector *v = NumericRangeTree_Find(t, rngs[r].min, rngs[r].max);
    ASSERT(Vector_Size(v) > 0);
    // printf("Got %d ranges for %f..%f...\n", Vector_Size(v), rngs[r].min, rngs[r].max);
    for (int i = 0; i < Vector_Size(v); i++) {
      NumericRange *l;
      Vector_Get(v, i, &l);
      ASSERT(l);
      // printf("%f...%f\n", l->minVal, l->maxVal);
      ASSERT(!(l->minVal > rngs[r].max));
      ASSERT(!(l->maxVal < rngs[r].min));
    }
    Vector_Free(v);
  }
  NumericRangeTree_Free(t);
  return 0;
}

int testRangeIterator() {
  NumericRangeTree *t = NewNumericRangeTree();
  ASSERT(t != NULL);

  int N = 100;
  for (int i = 0; i < N; i++) {

    t_docId docId = i + 1;
    float value = (double)(1 + prng() % 1000);
    // printf("Adding %d > %f\n", docId, value);
    NumericRangeTree_Add(t, docId, value);
  }
  ASSERT_EQUAL(t->numRanges, 4);
  ASSERT_EQUAL(t->numEntries, N);

  NumericFilter *flt = NewNumericFilter(-1, 1002, 0, 0);
  IndexIterator *it = NewNumericFilterIterator(t, flt);
  ASSERT(it->HasNext(it->ctx));

  // ASSERT_EQUAL(it->Len(it->ctx), N);
  int count = 0;

  RSIndexResult *res = NULL;
  while (it->HasNext(it->ctx)) {

    int rc = it->Read(it->ctx, &res);
    if (rc == INDEXREAD_EOF) {
      break;
    }

    ASSERT_EQUAL(res->type, RSResultType_Virtual);
    // ASSERT_EQUAL(res->agg.typeMask, RSResultType_Virtual);
    ASSERT(!RSIndexResult_HasOffsets(res));
    ASSERT(!RSIndexResult_IsAggregate(res));
    ASSERT(res->docId > 0);
    ASSERT_EQUAL(res->fieldMask, RS_FIELDMASK_ALL);

    count++;
  }
  it->Free(it);
  free(flt);
  ASSERT_EQUAL(N, count);
  // IndexResult_Free(&res);
  NumericRangeTree_Free(t);
  return 0;
}

int benchmarkNumericRangeTree() {
  NumericRangeTree *t = NewNumericRangeTree();
  int count = 1;
  for (int i = 0; i < 1000000; i++) {

    count += NumericRangeTree_Add(t, i, (double)(rand() % 100000));
  }
  printf("created %d range leaves\n", count);
  Vector *v;
  TIME_SAMPLE_RUN_LOOP(1000, {
    v = NumericRangeTree_Find(t, 1000, 20000);
    // printf("%d\n", v->top);
    Vector_Free(v);
  });

  TimeSample ts;

  NumericFilter *flt = NewNumericFilter(1000, 5000, 0, 0);
  IndexIterator *it = NewNumericFilterIterator(t, flt);
  ASSERT(it->HasNext(it->ctx));

  // ASSERT_EQUAL(it->Len(it->ctx), N);
  count = 0;

  RSIndexResult *res = NULL;
  TimeSampler_Start(&ts);
  while (it->HasNext(it->ctx)) {

    int rc = it->Read(it->ctx, &res);
    TimeSampler_Tick(&ts);
    // IndexResult_Print(res, 0);
    // if (rc == INDEXREAD_EOF) {
    //   break;
    // }
    ++count;
  }
  TimeSampler_End(&ts);

  printf("%d iteration of range iterators in %lld ns, %fns/iteration", count,
         TimeSampler_DurationNS(&ts), TimeSampler_IterationMS(&ts) * 1000000);

  it->Free(it);

  NumericRangeTree_Free(t);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testNumericRangeTree);
  TESTFUNC(testRangeIterator);
  // benchmarkNumericRangeTree();
});
