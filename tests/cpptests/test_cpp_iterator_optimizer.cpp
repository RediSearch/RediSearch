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
#include "common.h"
#include "index_utils.h"
#include "spec.h"
#include "notifications.h"

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

// `OPT_Rewind` drops the numeric iterator once the filter has no range left;
// the estimate must still be answerable then, as suspending the tree asks for it.
TEST_F(OptimizerIteratorTest, NumEstimatedWithoutNumericIterator) {
  RMCK::Context ctx;
  Initialize_KeyspaceNotifications();
  RefManager *ism = createSpec(ctx);
  RediSearch_CreateNumericField(ism, "n");
  for (unsigned i = 1; i <= 10; i++) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(i).c_str(), "n", std::to_string(i).c_str()));
  }
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));

  QOptimizer *opt = QOptimizer_New();
  opt->sctx = &sctx;
  opt->fieldName = "n";
  opt->limit = 5;
  opt->asc = true;

  QueryIterator *child = NewWildcardIterator_NonOptimized(10, 1.0);
  QueryIterator *optIter = NewOptimizerIterator(opt, child, &config);
  ASSERT_EQ(optIter->type, OPTIMUS_ITERATOR);

  OptimizerIterator *oi = (OptimizerIterator *)optIter;
  oi->numericIter->Free(oi->numericIter);
  oi->numericIter = NULL;

  EXPECT_EQ(optIter->NumEstimated(optIter), child->NumEstimated(child));

  optIter->Free(optIter);
  QOptimizer_Free(opt);
  freeSpec(ism);
}
