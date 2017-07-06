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

#define _min(x, y) (x < y ? x : y)
#define _max(x, y) (x < y ? y : x)

int testRangeIterator() {
  NumericRangeTree *t = NewNumericRangeTree();
  ASSERT(t != NULL);

  int N = 1000000;
  double *lookup = calloc(N + 1, sizeof(double));
  uint8_t *matched = calloc(N + 1, sizeof(uint8_t));
  for (int i = 0; i < N; i++) {

    t_docId docId = i + 1;
    float value = (double)(1 + prng() % (N / 5));
    lookup[docId] = value;
    // printf("Adding %d > %f\n", docId, value);
    NumericRangeTree_Add(t, docId, value);
  }

  for (int i = 0; i < 5; i++) {
    double min = (double)(1 + prng() % (N / 5));
    double max = (double)(1 + prng() % (N / 5));
    memset(matched, 0, sizeof(uint8_t) * (N + 1));
    NumericFilter *flt = NewNumericFilter(_min(min, max), _max(min, max), 1, 1);

    // count the number of elements in the range
    int count = 0;
    for (int i = 1; i <= N; i++) {
      if (NumericFilter_Match(flt, lookup[i])) {
        matched[i] = 1;
        count++;
      }
    }

    // printf("Testing range %f..%f, should have %d docs\n", min, max, count);
    IndexIterator *it = NewNumericFilterIterator(t, flt);

    int xcount = 0;
    RSIndexResult *res = NULL;

    while (it->HasNext(it->ctx)) {

      int rc = it->Read(it->ctx, &res);
      if (rc == INDEXREAD_EOF) {
        break;
      }
      ASSERT(matched[res->docId] == 1);
      matched[res->docId] = (uint8_t)2;
      // printf("rc: %d docId: %d, lookup %f, flt %f..%f\n", rc, res->docId, lookup[res->docId],
      //        flt->min, flt->max);

      ASSERT(NumericFilter_Match(flt, lookup[res->docId]));

      ASSERT_EQUAL(res->type, RSResultType_Virtual);
      // ASSERT_EQUAL(res->agg.typeMask, RSResultType_Virtual);
      ASSERT(!RSIndexResult_HasOffsets(res));
      ASSERT(!RSIndexResult_IsAggregate(res));
      ASSERT(res->docId > 0);
      ASSERT_EQUAL(res->fieldMask, RS_FIELDMASK_ALL);

      xcount++;
    }
    for (int i = 1; i <= N; i++) {
      if (matched[i] == 1) {
        printf("Miss: %d\n", i);
      }
    }

    // printf("The iterator returned %d elements\n", xcount);
    ASSERT_EQUAL(xcount, count);
    it->Free(it);
  }
  free(lookup);

  ASSERT_EQUAL(t->numRanges, 142);
  ASSERT_EQUAL(t->numEntries, N);
  NumericRangeTree_Free(t);

  return 0;
}

int benchmarkNumericRangeTree() {
  NumericRangeTree *t = NewNumericRangeTree();
  int count = 1;
  for (int i = 0; i < 100000; i++) {

    count += NumericRangeTree_Add(t, i, (double)(rand() % 500000));
  }
  // printf("created %d range leaves\n", count);

  TIME_SAMPLE_RUN_LOOP(1000, {
    Vector *v = NumericRangeTree_Find(t, 1000, 20000);
    // printf("%d\n", v->top);
    Vector_Free(v);
  });

  TimeSample ts;

  NumericFilter *flt = NewNumericFilter(1000, 50000, 0, 0);
  IndexIterator *it = NewNumericFilterIterator(t, flt);
  ASSERT(it->HasNext(it->ctx));

  // ASSERT_EQUAL(it->Len(it->ctx), N);
  count = 0;

  RSIndexResult *res = NULL;

  it->Free(it);

  NumericRangeTree_Free(t);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();

  TESTFUNC(testNumericRangeTree);
  TESTFUNC(testRangeIterator);
  benchmarkNumericRangeTree();
});
