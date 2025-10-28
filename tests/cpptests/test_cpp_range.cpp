/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"

#include "numeric_index.h"
#include "rmutil/alloc.h"
#include "index_utils.h"
#include "redisearch_api.h"
#include "common.h"

#include <stdio.h>
#include <random>
#include <unordered_set>

extern "C" {
// declaration for an internal function implemented in numeric_index.c
QueryIterator *createNumericIterator(const RedisSearchCtx *sctx, NumericRangeTree *t,
                                     const NumericFilter *f, IteratorsConfig *config,
                                     const FieldFilterContext* filterCtx);
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
  ASSERT_EQ(t->numRanges, 8);
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
    NumericFilter *flt = NewNumericFilter(std::min(min, max), std::max(min, max), 1, 1, true, NULL);

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
    FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
    FieldFilterContext filterCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
    QueryIterator *it = createNumericIterator(NULL, t, flt, &config, &filterCtx);

    int xcount = 0;

    while (it->Read(it) == ITERATOR_OK) {
      RSIndexResult *res = it->current;

      size_t found_mult = -1;
      for( size_t mult = 0; mult < mult_count; ++mult) {
        if (matched[res->docId].v[mult] == 1) {
          matched[res->docId].v[mult] = (uint8_t)2;
          found_mult = mult; // at least one is matching
          xcount++;
        }
      }
      ASSERT_NE(found_mult, -1);
      if (res->data.tag == RSResultData_Union) {
        const RSAggregateResult *agg = IndexResult_AggregateRef(res);
        res = (RSIndexResult*)AggregateResult_Get(agg, 0);
      }

      // printf("rc: %d docId: %d, n %f lookup %f, flt %f..%f\n", rc, res->docId, res->num.value,
      //        lookup[res->docId], flt->min, flt->max);

      found_mult = -1;
      for( size_t mult = 0; mult < mult_count; ++mult) {
        if (IndexResult_NumValue(res) == lookup[res->docId].v[mult]) {
          ASSERT_TRUE(NumericFilter_Match(flt, lookup[res->docId].v[mult]));
          found_mult = mult;
        }
      }
      ASSERT_NE(found_mult, -1);

      ASSERT_EQ(res->data.tag, RSResultData_Numeric);
      ASSERT_TRUE(!RSIndexResult_HasOffsets(res));
      ASSERT_TRUE(!IndexResult_IsAggregate(res));
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

  ASSERT_EQ(t->numRanges, !isMulti ? 12 : 36);
  ASSERT_EQ(t->numEntries, !isMulti ? N : N * MULT_COUNT);


  // test loading limited range
  double rangeArray[6][2] = {{0, 1000}, {0, 3000}, {1000, 3000}, {15000, 20000}, {19500, 20000}, {-1000, 21000}};

  FieldFilterContext filterCtx = {.field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}}, .predicate = FIELD_EXPIRATION_DEFAULT};
  for (size_t i = 0; i < 6; i++) {
    for (int j = 0; j < 2; ++j) {
      // j==1 for ascending order, j==0 for descending order
      NumericFilter *flt = NewNumericFilter(rangeArray[i][0], rangeArray[i][1], 1, 1, j, NULL);
      QueryIterator *it = createNumericIterator(NULL, t, flt, &config, &filterCtx);
      size_t numEstimated = it->NumEstimated(it);
      NumericFilter *fltLimited = NewNumericFilter(rangeArray[i][0], rangeArray[i][1], 1, 1, j, NULL);
      fltLimited->limit = 50;
      QueryIterator *itLimited = createNumericIterator(NULL, t, fltLimited, &config, &filterCtx);
      size_t numEstimatedLimited = itLimited->NumEstimated(itLimited);
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

/** Currently, a new tree always initialized with a single range node (root).
 * A range node contains an inverted index struct and at least one block with initial block capacity.
 */
TEST_F(RangeTest, EmptyTreeSanity) {
  NumericRangeNode *failed_range = NULL;

  NumericRangeTree *rt = NewNumericRangeTree();
  // The base inverted index is 32 bytes + 8 bytes for the entries count of numeric records
  // And IndexBlock is also 48 bytes
  // And initial block capacity of 6 bytes
  size_t empty_numeric_mem_size = 40 + 48 + 6;
  size_t numeric_tree_mem = CalculateNumericInvertedIndexMemory(rt, &failed_range);
  if (failed_range) {
    FAIL();
  }

  ASSERT_EQ(numeric_tree_mem, empty_numeric_mem_size);
  ASSERT_EQ(numeric_tree_mem, rt->invertedIndexesSize);

  NumericRangeTree_Free(rt);
}

class RangeIndexTest : public ::testing::Test {
protected:
  RefManager *index;
  RMCK::Context ctx;

  void SetUp() override {
    RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec = 3000000;
    index = createSpec(ctx);
  }

  void TearDown() override {
    IndexSpec_RemoveFromGlobals({index}, false);
  }
};

/** This test purpose is to verify the invertedIndexesSize member of the tree struct properly captures the sum of
 * all the inverted indexes in the tree.
 */
TEST_F(RangeIndexTest, testNumericTreeMemory) {
  size_t num_docs = 1000;

  // adding the numeric field to the index
  const char *numeric_field_name = "n";
  RediSearch_CreateNumericField(index, numeric_field_name);

  std::mt19937 gen(42);
  std::uniform_int_distribution<size_t> dis(0, num_docs - 1);
  std::unordered_set<size_t> generated_numbers;

  size_t expected_mem = 0;
  size_t last_added_mem = 0;
  NumericRangeNode *failed_range = NULL;

  auto print_failure = [&]() {
    std::cout << "Expected range memory = " << expected_mem << std::endl;
    std::cout << "Failed range mem: " << InvertedIndex_MemUsage(failed_range->range->entries) << std::endl;
  };

  // add docs with random numbers
  for (size_t i = 0 ; i < num_docs ; i++) {
    size_t random_val = dis(gen);
    generated_numbers.insert(random_val);
    std::string val_str = std::to_string(random_val);
    last_added_mem = ::addDocumentWrapper(ctx, index, numToDocStr(i).c_str(), numeric_field_name, val_str.c_str());
    expected_mem += last_added_mem;
  }

  // Get the numeric tree
  NumericRangeTree *rt = getNumericTree(get_spec(index), numeric_field_name);
  ASSERT_NE(rt, nullptr);

  // check memory
  size_t numeric_tree_mem = CalculateNumericInvertedIndexMemory(rt, &failed_range);
  ASSERT_EQ(rt->invertedIndexesSize, numeric_tree_mem);
  ASSERT_EQ(rt->invertedIndexesSize, expected_mem);

  if (failed_range) {
    print_failure();
    FAIL();
  }

  // delete some docs
  size_t deleted_docs = num_docs / 4;

  // Add random numbers if needed
  while (generated_numbers.size() < deleted_docs) {
      size_t random_val = dis(gen);
      generated_numbers.insert(random_val);
  }

  for (const size_t& random_id : generated_numbers) {
    auto rv = RS::deleteDocument(ctx, index, numToDocStr(random_id).c_str());
    ASSERT_TRUE(rv) << "Failed to delete doc " << random_id;
  }

  // config gc
  RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
  // Collect deleted docs
  GCContext *gc = get_spec(index)->gc;
  gc->callbacks.periodicCallback(gc->gcCtx);

  // check memory
  expected_mem = get_spec(index)->stats.invertedSize;
  numeric_tree_mem = CalculateNumericInvertedIndexMemory(rt, &failed_range);
  if (failed_range) {
    print_failure();
    FAIL();
  }
  ASSERT_EQ(rt->invertedIndexesSize, numeric_tree_mem);
  ASSERT_EQ(rt->invertedIndexesSize, expected_mem);

}

/**
 * Test the overhead of the numeric tree struct (not including the inverted indices memory)
 */
TEST_F(RangeIndexTest, testNumericTreeOverhead) {

  // Create index with multiple numeric indices
  RediSearch_CreateNumericField(index, "n1");
  RediSearch_CreateNumericField(index, "n2");

  // expect 0 overhead
  size_t overhead = IndexSpec_collect_numeric_overhead(get_spec(index));
  ASSERT_EQ(overhead, 0);

  // add docs to one field to trigger its index creation.
  ::addDocumentWrapper(ctx, index, numToDocStr(1).c_str(), "n1", "1");
  overhead = IndexSpec_collect_numeric_overhead(get_spec(index));
  ASSERT_EQ(overhead, sizeof(NumericRangeTree));

  // Delete the doc, the overhead shouldn't change
  auto rv = RS::deleteDocument(ctx, index, numToDocStr(1).c_str());
  ASSERT_TRUE(rv) << "Failed to delete doc1 ";

  // config gc
  RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
  // Collect deleted docs
  GCContext *gc = get_spec(index)->gc;
  gc->callbacks.periodicCallback(gc->gcCtx);

  overhead = IndexSpec_collect_numeric_overhead(get_spec(index));
  ASSERT_EQ(overhead, sizeof(NumericRangeTree));

  // Add a doc to trigger the creation of the second index
  ::addDocumentWrapper(ctx, index, numToDocStr(1).c_str(), "n1", "1");
  ::addDocumentWrapper(ctx, index, numToDocStr(2).c_str(), "n2", "1");
  overhead = IndexSpec_collect_numeric_overhead(get_spec(index));

  ASSERT_EQ(overhead, 2 * sizeof(NumericRangeTree));
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
