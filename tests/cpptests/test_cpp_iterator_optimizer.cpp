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
#include "iterator_util.h"

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

// The numeric filter covers documents 1 to `kNumDocs`, valued by their id; the child matches only
// the `kMatches` highest of them, plus `kPhantoms` ids the filter does not cover. The phantoms make
// the child's estimate exceed every numeric window, so the optimizer keeps widening the window to
// fill a heap of `kLimit` that the child can never fill, until `OPT_Rewind` finds no range left and
// drops the numeric iterator.
class OptimizerWithoutNumericIteratorTest : public OptimizerIteratorTest {
protected:
  static constexpr unsigned kNumDocs = 100;
  static constexpr unsigned kMatches = 5;
  static constexpr unsigned kPhantoms = 2 * kNumDocs;
  static constexpr size_t kLimit = 4 * kMatches;

  RMCK::Context ctx;
  RefManager *ism = nullptr;
  RedisSearchCtx sctx;
  QOptimizer *opt = nullptr;
  MockIterator *child = nullptr;
  QueryIterator *optIter = nullptr;

  void SetUp() override {
    OptimizerIteratorTest::SetUp();
    Initialize_KeyspaceNotifications();
    ism = createSpec(ctx);
    RediSearch_CreateNumericField(ism, "n");
    for (unsigned i = 1; i <= kNumDocs; i++) {
      ASSERT_TRUE(
          RS::addDocument(ctx, ism, numToDocStr(i).c_str(), "n", std::to_string(i).c_str()));
    }
    sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
    makeIterator();
  }

  void TearDown() override {
    freeIterator();
    freeSpec(ism);
  }

  // A fresh optimizer per iterator: one that owns its numeric filter frees it with the iterator but
  // leaves the optimizer pointing at it.
  void makeIterator() {
    freeIterator();
    opt = QOptimizer_New();
    opt->sctx = &sctx;
    opt->fieldName = "n";
    opt->limit = kLimit;
    opt->asc = true;

    child = new MockIterator();
    for (t_docId id = kNumDocs - kMatches + 1; id <= kNumDocs + kPhantoms; id++) {
      child->docIds.push_back(id);
    }
    optIter = NewOptimizerIterator(opt, &child->base, &config);
    ASSERT_EQ(optIter->type, OPTIMUS_ITERATOR);
  }

  void freeIterator() {
    if (optIter) {
      // Freeing the optimizer does not give back the document references of results it never
      // yielded, so hand them out first.
      while (optIter->Read(optIter) == ITERATOR_OK) {
        DMD_Return(optIter->current->dmd);
      }
      optIter->Free(optIter);
      optIter = nullptr;
    }
    if (opt) {
      QOptimizer_Free(opt);
      opt = nullptr;
    }
  }

  OptimizerIterator *oi() const {
    return (OptimizerIterator *)optIter;
  }

  // Reads one result, giving back the document reference the optimizer took for it.
  t_docId readOne() {
    if (optIter->Read(optIter) != ITERATOR_OK) {
      ADD_FAILURE() << "expected a result";
      return 0;
    }
    DMD_Return(optIter->current->dmd);
    return optIter->current->docId;
  }

  // Reads the remaining results and checks that, with `first`, they are exactly the child's
  // matches. The heap yields them in no particular order; the pipeline sorts them downstream.
  void expectRestAreMatches(t_docId first) {
    std::vector<t_docId> ids{first};
    while (optIter->Read(optIter) == ITERATOR_OK) {
      ids.push_back(optIter->current->docId);
      DMD_Return(optIter->current->dmd);
    }
    std::sort(ids.begin(), ids.end());
    std::vector<t_docId> matches;
    for (t_docId id = kNumDocs - kMatches + 1; id <= kNumDocs; id++) {
      matches.push_back(id);
    }
    EXPECT_EQ(ids, matches);
  }
};

// Suspending the tree snapshots its estimate, so it must still be answerable once the numeric
// iterator is gone; the child then bounds it alone.
TEST_F(OptimizerWithoutNumericIteratorTest, NumEstimated) {
  t_docId first = readOne();
  ASSERT_EQ(oi()->numericIter, nullptr);

  EXPECT_EQ(optIter->NumEstimated(optIter), child->NumEstimated());
  expectRestAreMatches(first);
}

// The query iterator processor revalidates the tree whenever it re-takes the lock, including after
// the numeric iterator is gone. The child's status is then the optimizer's, with `MOVED` folded
// into `ABORTED` as for any child, and a still-valid tree keeps yielding what it collected.
TEST_F(OptimizerWithoutNumericIteratorTest, RevalidateFollowsChild) {
  const std::pair<ValidateStatus, ValidateStatus> cases[] = {
      {VALIDATE_OK, VALIDATE_OK},
      {VALIDATE_TIMEOUT, VALIDATE_TIMEOUT},
      {VALIDATE_ABORTED, VALIDATE_ABORTED},
      {VALIDATE_MOVED, VALIDATE_ABORTED},
  };

  for (const auto &[childStatus, expected] : cases) {
    SCOPED_TRACE(childStatus);
    // A fresh iterator per case: an aborted or timed-out tree must be freed, not revalidated again.
    makeIterator();
    t_docId first = readOne();
    ASSERT_EQ(oi()->numericIter, nullptr);
    child->SetRevalidateResult(childStatus);

    IndexSpec_LockRead(sctx.spec);
    EXPECT_EQ(optIter->Revalidate(optIter, sctx.spec), expected);
    IndexSpec_Unlock(sctx.spec);
    EXPECT_EQ(child->GetValidationCount(), 1);

    if (expected == VALIDATE_OK) {
      expectRestAreMatches(first);
    }
  }
}

// Rewinding after the ranges ran out must not need the numeric iterator either.
TEST_F(OptimizerWithoutNumericIteratorTest, Rewind) {
  t_docId first = readOne();
  ASSERT_EQ(oi()->numericIter, nullptr);

  optIter->Rewind(optIter);
  EXPECT_EQ(oi()->numericIter, nullptr);
  expectRestAreMatches(first);
}
