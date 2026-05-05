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
#include "src/optimizer_reader.h"
#include "src/query_optimizer.h"

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

  IndexIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  ASSERT_NE(child, nullptr);

  IndexIterator *optIter = NewOptimizerIterator(opt, child, &config);

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

  IndexIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  ASSERT_NE(child, nullptr);

  IndexIterator *optIter = NewOptimizerIterator(opt, child, &config);

  // Should return NULL due to multiplication overflow check
  EXPECT_EQ(optIter, nullptr);

  child->Free(child);
  QOptimizer_Free(opt);
}
