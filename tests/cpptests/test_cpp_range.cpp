#include <gtest/gtest.h>
#include "numeric_index.h"
#include <stdio.h>
// #include "time_sample.h"
#include "index.h"
#include "rmutil/alloc.h"

extern "C" {
// declaration for an internal function implemented in numeric_index.c
IndexIterator *createNumericIterator(const IndexSpec* sp, NumericRangeSkiplist *nrsl, const NumericFilter *f);
}

// Helper so we get the same pseudo-random numbers
// in tests across environments
unsigned prng_seed = 1337;
#define PRNG_MOD 30980347
unsigned prng() {
  prng_seed = (prng_seed * prng_seed) % PRNG_MOD;
  return prng_seed;
}

class RangeTest : public ::testing::Test {};

TEST_F(RangeTest, testRangeSkiplist) {
  NumericRangeSkiplist *nrsl = NewNumericRangeSkiplist();
  ASSERT_TRUE(nrsl != NULL);

  for (size_t i = 0; i < 50000; i++) {

    NumericRangeSkiplist_Add(nrsl, i + 1, (double)(1 + prng() % 5000));
  }
  ASSERT_EQ(nrsl->numRanges, 16);
  ASSERT_EQ(nrsl->numEntries, 50000);

  struct {
    double min;
    double max;
  } rngs[] = {{0, 100}, {10, 1000}, {2500, 3500}, {0, 5000}, {4999, 4999}, {0, 0}};

  for (int r = 0; rngs[r].min || rngs[r].max; r++) {

    Vector *v = NumericRangeSkiplist_Find(nrsl, rngs[r].min, rngs[r].max);
    ASSERT_TRUE(Vector_Size(v) > 0);
    // printf("Got %d ranges for %f..%f...\n", Vector_Size(v), rngs[r].min, rngs[r].max);
    for (int i = 0; i < Vector_Size(v); i++) {
      NumericRange *l;
      Vector_Get(v, i, &l);
      ASSERT_TRUE(l);
      // printf("%f...%f\n", l->minVal, l->maxVal);
      ASSERT_FALSE(l->minVal > rngs[r].max);
      ASSERT_FALSE(l->maxVal < rngs[r].min);
    }
    Vector_Free(v);
  }
  NumericRangeSkiplist_Free(nrsl);
}

TEST_F(RangeTest, testRangeIterator) {
  NumericRangeSkiplist *nrsl = NewNumericRangeSkiplist();
  ASSERT_TRUE(nrsl != NULL);

  const size_t N = 100000;
  std::vector<double> lookup;
  std::vector<uint8_t> matched;
  lookup.resize(N + 1);
  matched.resize(N + 1);
  for (size_t i = 0; i < N; i++) {
    t_docId docId = i + 1;
    double value = (double)(1 + prng() % (N / 5));
    lookup[docId] = value;
    // printf("Adding %d > %f\n", docId, value);
    NumericRangeSkiplist_Add(nrsl, docId, value);
  }

  for (size_t i = 0; i < 5; i++) {
    double min = (double)(1 + prng() % (N / 5));
    double max = (double)(1 + prng() % (N / 5));
    memset(&matched[0], 0, sizeof(uint8_t) * (N + 1));
    NumericFilter *flt = NewNumericFilter(std::min(min, max), std::max(min, max), 1, 1);

    // count the number of elements in the range
    size_t count = 0;
    for (size_t i = 1; i <= N; i++) {
      if (NumericFilter_Match(flt, lookup[i])) {
        matched[i] = 1;
        count++;
      }
    }

    // printf("Testing range %f..%f, should have %d docs\n", min, max, count);
    IndexIterator *it = createNumericIterator(NULL, nrsl, flt);

    int xcount = 0;
    RSIndexResult *res = NULL;

    while (IITER_HAS_NEXT(it)) {

      int rc = it->Read(it->ctx, &res);
      if (rc == INDEXREAD_EOF) {
        break;
      }

      ASSERT_EQ(matched[res->docId], 1);
      if (res->type == RSResultType_Union) {
        res = res->agg.children[0];
      }

      matched[res->docId] = (uint8_t)2;
      // printf("rc: %d docId: %d, n %f lookup %f, flt %f..%f\n", rc, res->docId, res->num.value,
      //        lookup[res->docId], flt->min, flt->max);

      ASSERT_EQ(res->num.value, lookup[res->docId]);

      ASSERT_TRUE(NumericFilter_Match(flt, lookup[res->docId]));

      ASSERT_EQ(res->type, RSResultType_Numeric);
      // ASSERT_EQUAL(res->agg.typeMask, RSResultType_Virtual);
      ASSERT_TRUE(!RSIndexResult_HasOffsets(res));
      ASSERT_TRUE(!RSIndexResult_IsAggregate(res));
      ASSERT_TRUE(res->docId > 0);
      ASSERT_EQ(res->fieldMask, RS_FIELDMASK_ALL);

      xcount++;
    }
    for (int i = 1; i <= N; i++) {
      if (matched[i] == 1) {
        printf("Miss: %d\n", i);
      }
    }

    // printf("The iterator returned %d elements\n", xcount);
    ASSERT_EQ(xcount, count);
    it->Free(it);
    NumericFilter_Free(flt);
  }

  ASSERT_EQ(nrsl->numRanges, 14);
  ASSERT_EQ(nrsl->numEntries, N);
  NumericRangeSkiplist_Free(nrsl);
}

// int benchmarkNumericRangeSkiplist() {
//   NumericRangeSkiplist *nrsl = NewNumericRangeSkiplist();
//   int count = 1;
//   for (int i = 0; i < 100000; i++) {

//     count += NumericRangeSkiplist_Add(nrsl, i, (double)(rand() % 500000));
//   }
//   // printf("created %d range leaves\n", count);

//   TIME_SAMPLE_RUN_LOOP(1000, {
//     Vector *v = NumericRangeSkiplist_Find(nrsl, 1000, 20000);
//     // printf("%d\n", v->top);
//     Vector_Free(v);
//   });

//   TimeSample ts;

//   NumericFilter *flt = NewNumericFilter(1000, 50000, 0, 0);
//   IndexIterator *it = createNumericIterator(nrsl, flt);
//   ASSERT(it->HasNext(it->ctx));

//   // ASSERT_EQUAL(it->Len(it->ctx), N);
//   count = 0;

//   RSIndexResult *res = NULL;

//   it->Free(it);

//   NumericRangeSkiplist_Free(nrsl);
//   NumericFilter_Free(flt);
//   return 0;
// }
