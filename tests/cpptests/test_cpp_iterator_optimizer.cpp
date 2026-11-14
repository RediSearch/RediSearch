/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/iterators/optimizer_reader.h"
#include "src/query_optimizer.h"
#include "iterators_ffi.h"
#include "index_utils.h"
#include "numeric_range_tree_ffi.h"
#include "rqe_iterator_type.h"

#include <memory>

class OptimizerIteratorTest : public ::testing::Test {
protected:
  IteratorsConfig config;

  void SetUp() override {
    // Initialize with default config
    memset(&config, 0, sizeof(config));
  }
};

// Test 1: Addition overflow (limit + 1 overflows)
TEST_F(OptimizerIteratorTest, AdditionOverflowReturnsNull) {
  // When limit = SIZE_MAX, then limit + 1 overflows
  QOptimizer *opt = QOptimizer_New();
  ASSERT_NE(opt, nullptr);

  opt->limit = SIZE_MAX;  // limit + 1 will overflow
  opt->asc = true;

  QueryIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  ASSERT_NE(child, nullptr);

  QueryIterator *optIter = NewOptimizerIterator(opt, child, &config);

  // Should return NULL due to addition overflow check
  EXPECT_EQ(optIter, nullptr);

  child->Free(child);
  QOptimizer_Free(opt);
}

// Test 2: Multiplication overflow ((limit + 1) * sizeof(RSIndexResult) overflows)
TEST_F(OptimizerIteratorTest, MultiplicationOverflowReturnsNull) {
  // Use a limit where limit + 1 is valid, but multiplication overflows
  // SIZE_MAX / sizeof(RSIndexResult) is the boundary
  QOptimizer *opt = QOptimizer_New();
  ASSERT_NE(opt, nullptr);

  // This limit won't overflow on +1, but will overflow on multiplication
  opt->limit = SIZE_MAX / sizeof(RSIndexResult);
  opt->asc = true;

  QueryIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  ASSERT_NE(child, nullptr);

  QueryIterator *optIter = NewOptimizerIterator(opt, child, &config);

  // Should return NULL due to multiplication overflow check
  EXPECT_EQ(optIter, nullptr);

  child->Free(child);
  QOptimizer_Free(opt);
}

TEST_F(OptimizerIteratorTest, MatchEstimateCallbackAndFallback) {
  QueryIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  ASSERT_NE(child->EstimatedMatches, nullptr);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 100), 10.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 3), 3.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 0), 0.0);

  child->EstimatedMatches = nullptr;
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 100), 10.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 3), 3.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(child, 0), 0.0);
  child->Free(child);
}

TEST_F(OptimizerIteratorTest, MatchEstimateTracksNumericWindowAvailability) {
  MockQueryEvalCtx context(100, 100);
  auto freeName = [](const HiddenString *name) { HiddenString_Free(name, false); };
  std::unique_ptr<HiddenString, decltype(freeName)> fieldName(
      NewHiddenString("price", sizeof("price") - 1, false), freeName);
  FieldSpec field = {};
  field.fieldName = fieldName.get();
  field.types = INDEXFLD_T_NUMERIC;
  field.index = 0;
  context.spec.fields = &field;
  context.spec.numFields = 1;
  std::unique_ptr<NumericRangeTree, decltype(&NumericRangeTree_Free)> tree(
      openNumericOrGeoIndex(&context.spec, &field, true), &NumericRangeTree_Free);
  ASSERT_NE(tree, nullptr);
  for (t_docId docId = 1; docId <= 10; ++docId) {
    _NumericRangeTree_Add(tree.get(), docId, static_cast<double>(docId - 1), false, 0, 0);
  }

  NumericFilter filter = {};
  filter.min = 0;
  filter.max = 4.5;
  filter.minInclusive = true;
  filter.maxInclusive = true;
  filter.ascending = true;
  filter.fieldIndex = 0;
  QOptimizer options = {};
  options.sctx = &context.sctx;
  options.fieldName = "price";
  options.nf = &filter;
  options.limit = 3;
  options.asc = true;
  auto freeIterator = [](QueryIterator *iterator) { iterator->Free(iterator); };
  std::unique_ptr<QueryIterator, decltype(freeIterator)> child(
      NewWildcardIterator_NonOptimized(50, 1.0), freeIterator);
  std::unique_ptr<QueryIterator, decltype(freeIterator)> optimizer(
      NewOptimizerIterator(&options, child.release(), &config), freeIterator);
  ASSERT_NE(optimizer, nullptr);
  ASSERT_EQ(optimizer->type, OPTIMUS_ITERATOR);
  ASSERT_NE(optimizer->EstimatedMatches, nullptr);

  // The filter covers half a numeric range; the child selects half the corpus.
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 100), 2.5);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 20), 3.0);
  EXPECT_EQ(optimizer->NumEstimated(optimizer.get()), 10);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 0), 0.0);

  optimizer->Rewind(optimizer.get());
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 100), 3.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 3), 3.0);
  EXPECT_DOUBLE_EQ(QueryIterator_EstimatedMatches(optimizer.get(), 0), 0.0);
}
