
#include "gtest/gtest.h"

#include "numeric_index.h"
#include "index.h"
#include "rmutil/alloc.h"

#include <stdio.h>

extern "C" {
// declaration for an internal function implemented in numeric_index.c
IndexIterator *createNumericIterator(const RedisSearchCtx *sctx, NumericRangeTree *t,
                                     const NumericFilter *f, IteratorsConfig *config,
                                     const FieldIndexFilterContext* filterCtx);
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

TEST_F(RangeTest, testRangeTree) {
  NumericRangeTree *t = NewNumericRangeTree();
  ASSERT_TRUE(t != NULL);

  for (size_t i = 0; i < 50000; i++) {

    NumericRangeTree_Add(t, i + 1, (double)(1 + prng() % 5000), false);
  }
  ASSERT_EQ(t->numRanges, 12);
  ASSERT_EQ(t->numEntries, 50000);

  struct {
    double min;
    double max;
  } rngs[] = {{0, 100}, {10, 1000}, {2500, 3500}, {0, 5000}, {4999, 4999}, {0, 0}};

  for (int r = 0; rngs[r].min || rngs[r].max; r++) {
    NumericFilter nf = { .min = rngs[r].min, .max = rngs[r].max };
    Vector *v = NumericRangeTree_Find(t, &nf);
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
  NumericRangeTree_Free(t);
}

const size_t MULT_COUNT = 3;
struct d_arr {
  double v[MULT_COUNT];
};
struct uint8_arr {
  uint8_t v[MULT_COUNT];
};

void testRangeIteratorHelper(bool isMulti) {
  NumericRangeTree *t = NewNumericRangeTree();
  ASSERT_TRUE(t != NULL);

  
  const size_t N = 100000;
  std::vector<d_arr> lookup;
  std::vector<uint8_arr> matched;
  lookup.resize(N + 1);
  matched.resize(N + 1);
  size_t mult_count = (!isMulti ? 1 : MULT_COUNT);
  for (size_t i = 0; i < N; i++) {
    t_docId docId = i + 1;
    for( size_t mult = 0; mult < mult_count; ++mult) {
      double value = (double)(1 + prng() % (N / 5));
      lookup[docId].v[mult] = value;
      // printf("Adding %ld > %f\n", docId, value);
      NumericRangeTree_Add(t, docId, value, isMulti);
    }
  }

    IteratorsConfig config{};
    iteratorsConfig_init(&config);
  for (size_t i = 0; i < 5; i++) {
    double min = (double)(1 + prng() % (N / 5));
    double max = (double)(1 + prng() % (N / 5));
    memset(&matched[0], 0, sizeof(uint8_t) * (N + 1));
    NumericFilter *flt = NewNumericFilter(std::min(min, max), std::max(min, max), 1, 1, true);

    // count the number of elements in the range
    size_t count = 0;
    for (size_t i = 1; i <= N; i++) {
      for( size_t mult = 0; mult < mult_count; ++mult) {
        if (NumericFilter_Match(flt, lookup[i].v[mult])) {
          // Mark as being in filter range
          matched[i].v[mult] = 1;
          count++;
          // printf("count %lu, i %lu, mult %lu, val %f\n", count, i, mult, lookup[i].v[mult]);
        }
      }
    }
    // printf("Testing range %f..%f, should have %d docs\n", min, max, count);
    FieldIndexFilterContext filterCtx = {.fieldIndex = 0, .predicate = FIELD_EXPIRATION_DEFAULT};
    IndexIterator *it = createNumericIterator(NULL, t, flt, &config, &filterCtx);

    int xcount = 0;
    RSIndexResult *res = NULL;

    while (IITER_HAS_NEXT(it)) {

      int rc = it->Read(it->ctx, &res);
      if (rc == INDEXREAD_EOF) {
        break;
      }

      size_t found_mult = -1;
      for( size_t mult = 0; mult < mult_count; ++mult) {
        if (matched[res->docId].v[mult] == 1) {
          matched[res->docId].v[mult] = (uint8_t)2;
          found_mult = mult; // at least one is matching
          xcount++;
        }
      }
      ASSERT_NE(found_mult, -1);
      if (res->type == RSResultType_Union) {
        res = res->agg.children[0];
      }

      // printf("rc: %d docId: %d, n %f lookup %f, flt %f..%f\n", rc, res->docId, res->num.value,
      //        lookup[res->docId], flt->min, flt->max);

      found_mult = -1;
      for( size_t mult = 0; mult < mult_count; ++mult) {
        if (res->num.value == lookup[res->docId].v[mult]) {
          ASSERT_TRUE(NumericFilter_Match(flt, lookup[res->docId].v[mult]));
          found_mult = mult;
        }
      }
      ASSERT_NE(found_mult, -1);
      
      ASSERT_EQ(res->type, RSResultType_Numeric);
      // ASSERT_EQUAL(res->agg.typeMask, RSResultType_Virtual);
      ASSERT_TRUE(!RSIndexResult_HasOffsets(res));
      ASSERT_TRUE(!RSIndexResult_IsAggregate(res));
      ASSERT_TRUE(res->docId > 0);
      ASSERT_EQ(res->fieldMask, RS_FIELDMASK_ALL);      
    }

    for (int i = 1; i <= N; i++) {
      bool missed = false;
      for( size_t mult = 0; mult < mult_count; ++mult) {
        auto match = matched[i].v[mult];
        if (match == 2) {
          missed = false;
          break;
        } else if (match == 1) {
          missed = true;
          // Keep trying - could be found
        }
      }
      
      if (missed) {
        printf("Miss: %d\n", i);
      }
    }

    // printf("The iterator returned %d elements\n", xcount);
    ASSERT_EQ(xcount, count);
    it->Free(it);
    NumericFilter_Free(flt);
  }

  ASSERT_EQ(t->numRanges, !isMulti ? 14 : 42);
  ASSERT_EQ(t->numEntries, !isMulti ? N : N * MULT_COUNT);


  // test loading limited range
  double rangeArray[6][2] = {{0, 1000}, {0, 3000}, {1000, 3000}, {15000, 20000}, {19500, 20000}, {-1000, 21000}}; 

  FieldIndexFilterContext filterCtx = {.fieldIndex = 0, .predicate = FIELD_EXPIRATION_DEFAULT};
  for (size_t i = 0; i < 6; i++) {
    for (int j = 0; j < 2; ++j) {   
      // j==1 for ascending order, j==0 for descending order
      NumericFilter *flt = NewNumericFilter(rangeArray[i][0], rangeArray[i][1], 1, 1, j);
      IndexIterator *it = createNumericIterator(NULL, t, flt, &config, &filterCtx);
      size_t numEstimated = it->NumEstimated(it->ctx);
      NumericFilter *fltLimited = NewNumericFilter(rangeArray[i][0], rangeArray[i][1], 1, 1, j);
      fltLimited->limit = 50;
      IndexIterator *itLimited = createNumericIterator(NULL, t, fltLimited, &config, &filterCtx);
      size_t numEstimatedLimited = itLimited->NumEstimated(itLimited->ctx);
      // printf("%f %f %ld %ld\n", rangeArray[i][0], rangeArray[i][1], numEstimated, numEstimatedLimited);
      ASSERT_TRUE(numEstimated >= numEstimatedLimited );
      it->Free(it);
      NumericFilter_Free(flt);
      itLimited->Free(itLimited);
      NumericFilter_Free(fltLimited);
    }
  }

  NumericRangeTree_Free(t);
}

TEST_F(RangeTest, testRangeIterator) {
  testRangeIteratorHelper(false);
}

TEST_F(RangeTest, testRangeIteratorMulti) {
  testRangeIteratorHelper(true);
}

// int benchmarkNumericRangeTree() {
//   NumericRangeTree *t = NewNumericRangeTree();
//   int count = 1;
//   for (int i = 0; i < 100000; i++) {

//     count += NumericRangeTree_Add(t, i, (double)(rand() % 500000));
//   }
//   // printf("created %d range leaves\n", count);

//   TIME_SAMPLE_RUN_LOOP(1000, {
//     Vector *v = NumericRangeTree_Find(t, 1000, 20000);
//     // printf("%d\n", v->top);
//     Vector_Free(v);
//   });

//   TimeSample ts;

//   NumericFilter *flt = NewNumericFilter(1000, 50000, 0, 0);
//   IndexIterator *it = createNumericIterator(t, flt);
//   ASSERT(it->HasNext(it->ctx));

//   // ASSERT_EQUAL(it->Len(it->ctx), N);
//   count = 0;

//   RSIndexResult *res = NULL;

//   it->Free(it);

//   NumericRangeTree_Free(t);
//   NumericFilter_Free(flt);
//   return 0;
// }
