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

// The limit OPT_Rewind gives a retry window: the estimator driven by the hit
// rate a window achieved, passed as the document count of that selectivity.
static size_t next_window_limit(size_t numDocs, size_t resultsMissing, double successRatio) {
  size_t observedEstimate = successRatio * numDocs;
  return QOptimizer_EstimateLimit(numDocs, observedEstimate, resultsMissing);
}

class OptimizerWindowSizingTest : public ::testing::Test {
protected:
  // A child selective enough that a window yields few matches, but not so few
  // that sizing is skipped in favour of reading every remaining document.
  static constexpr size_t numDocs = 10000;
  static constexpr size_t childEstimate = 200;
  static constexpr double hitRate = (double)childEstimate / numDocs;

  static constexpr size_t k = 100;
  static constexpr size_t windowRead = 1000;
  static constexpr size_t hitsCollected = (size_t)(windowRead * hitRate);
  static constexpr size_t resultsMissing = k - hitsCollected;
  static constexpr double successRatio = (double)hitsCollected / windowRead;
};

// Given the child's selectivity, the estimator sizes a window holding about as
// many matches as the heap still needs.
TEST_F(OptimizerWindowSizingTest, EstimatorSizesWindowToCoverMissingResults) {
  size_t estimate = QOptimizer_EstimateLimit(numDocs, childEstimate, resultsMissing);

  EXPECT_GT(estimate, windowRead);
  EXPECT_NEAR(estimate * hitRate, resultsMissing, 1.0);
}

// A retry window is large enough that, at the rate the previous window hit at,
// it holds every result the heap still needs, so it reads more than the window
// that came up short rather than less.
TEST_F(OptimizerWindowSizingTest, RetryWindowHoldsMissingResults) {
  size_t nextLimit = next_window_limit(numDocs, resultsMissing, successRatio);

  EXPECT_GT(nextLimit, windowRead);
  EXPECT_NEAR(nextLimit * successRatio, resultsMissing, 1.0);
}

// A lower observed hit rate yields a larger window: the measured rate, not the
// child's static estimate, sets the retry size.
TEST_F(OptimizerWindowSizingTest, RetryWindowIgnoresStaleChildEstimate) {
  double observedRate = successRatio / 4;
  size_t nextLimit = next_window_limit(numDocs, resultsMissing, observedRate);

  EXPECT_GT(nextLimit, next_window_limit(numDocs, resultsMissing, successRatio));
  EXPECT_NEAR(nextLimit * observedRate, resultsMissing, 1.0);
}
