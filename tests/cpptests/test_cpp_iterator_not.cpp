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
#include "iterator_util.h"

#include <random>
#include <vector>

#include "src/iterators/not_iterator.h"
#include "src/iterators/wildcard_iterator.h"
#include "src/iterators/empty_iterator.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/inverted_index/inverted_index.h"
#include "index_utils.h"

class NotIteratorCommonTest : public ::testing::TestWithParam<std::tuple<std::vector<t_docId>, std::vector<t_docId>, std::optional<t_docId>, bool>> {
protected:
  std::vector<t_docId> childDocIds;
  std::vector<t_docId> wcDocIds;
  std::vector<t_docId> resultSet;
  std::optional<t_docId> opt_max_doc_id;
  t_docId maxDocId;
  QueryIterator *iterator_base;
  std::unique_ptr<MockQueryEvalCtx> mockQctx;
  bool optimized;
  void SetUp() override {
    // Get child document IDs from parameter
    std::tie(childDocIds, wcDocIds, opt_max_doc_id, optimized) = GetParam();
    // Consider wcDocIds only if optimized is true

    // Find the maximum document ID
    if (opt_max_doc_id.has_value()) {
      maxDocId = opt_max_doc_id.value();
    } else {
      maxDocId = *std::max_element(childDocIds.begin(), childDocIds.end());
      if (optimized) {
        for (auto id : wcDocIds) {
          maxDocId = std::max(maxDocId, id);
        }
      }
      maxDocId += 5; // Add some buffer
    }
    if (!optimized) {
      wcDocIds.clear();
      for (auto i = 1; i < maxDocId; i++) {
        wcDocIds.push_back(i);
      }
    }
    // Compute resultSet from maxDocId and childDocIds
    resultSet.clear();
    if (!optimized) {
      for (t_docId id = 1; id <= maxDocId; id++) {
        if (std::find(childDocIds.begin(), childDocIds.end(), id) == childDocIds.end()) {
          resultSet.push_back(id);
        }
      }
    } else {
      for (auto wcId : wcDocIds) {
        if (std::find(childDocIds.begin(), childDocIds.end(), wcId) == childDocIds.end()) {
          if (wcId < maxDocId) {
            resultSet.push_back(wcId);
          }
        }
      }
    }

    auto child = (QueryIterator *) new MockIterator(childDocIds);
    struct timespec timeout = {LONG_MAX, 999999999}; // Initialize with "infinite" timeout
    // Store the wildcard iterator in the NotIterator structure instead of directly in QueryIterator


    if (optimized) {
      std::vector<t_docId> wildcard = {1, 2, 3};
      mockQctx = std::make_unique<MockQueryEvalCtx>(wildcard);
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, &mockQctx->qctx);
      NotIterator *ni = (NotIterator *)iterator_base;
      ni->wcii->Free(ni->wcii);
      ni->wcii = (QueryIterator *) new MockIterator(wcDocIds);
    } else {
      mockQctx = std::make_unique<MockQueryEvalCtx>(maxDocId, maxDocId);
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, &mockQctx->qctx);
    }
  }

  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_P(NotIteratorCommonTest, Read) {
  if (optimized && opt_max_doc_id.has_value()) {
    GTEST_SKIP() << "For optimized version the maxDocId is not necessarily respected (smaller than the max of Child or WCDocId): Is it worth adding this check? return base->lastDocId < ni->maxDocId ? ITERATOR_EOF : ITERATOR_OK; ";
  }
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;

  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
    ASSERT_EQ(ni->base.current->docId, resultSet[i]);
    ASSERT_EQ(ni->base.lastDocId, resultSet[i]);
    ASSERT_FALSE(ni->base.atEOF);
    i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ni->base.atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(i, resultSet.size()) << "Expected to read " << resultSet.size() << " documents";
}

TEST_P(NotIteratorCommonTest, SkipToEOF) {
  // Test skipping beyond maxDocId returns EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  // Test returning after EOF gives EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 2), ITERATOR_EOF);
}

TEST_P(NotIteratorCommonTest, SkipToChildNotOK) {
  if (optimized && opt_max_doc_id.has_value()) {
    GTEST_SKIP() << "For optimized version the maxDocId is not necessarily respected (smaller than the max of Child or WCDocId): Is it worth adding this check? return base->lastDocId < ni->maxDocId ? ITERATOR_EOF : ITERATOR_OK; ";
  }
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  // Test skipping from 0
  for (t_docId id : childDocIds) {
    t_docId expectedId;
    for (auto rsId: resultSet) {
      if (rsId > id) {
        expectedId = rsId;
        break;
      }
    }
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_NE(rc, ITERATOR_OK);
    if (rc == ITERATOR_NOTFOUND) {
      ASSERT_GT(iterator_base->current->docId, id);
      ASSERT_GT(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else {
      ASSERT_EQ(rc, ITERATOR_EOF);
      ASSERT_TRUE(iterator_base->atEOF);
    }
  }

  // Test skipping from intermediate results
  iterator_base->Rewind(iterator_base);
  for (t_docId id : childDocIds) {
    if (iterator_base->atEOF) {
      break;
    }
    t_docId skipToId;
    if (id > iterator_base->lastDocId) {
      skipToId = id;
    } else {
      for (auto cid : childDocIds) {
        if (cid > iterator_base->lastDocId) {
          skipToId = cid;
          break;
        }
      }
    }
    t_docId expectedId;
    for (auto rsId: resultSet) {
      if (rsId > skipToId) {
        expectedId = rsId;
        break;
      }
    }

    NotIterator *ni = (NotIterator*) iterator_base;
    if (skipToId <= iterator_base->lastDocId) {
      break;
    }
    rc = iterator_base->SkipTo(iterator_base, skipToId);
    ASSERT_NE(rc, ITERATOR_OK);
    if (rc == ITERATOR_NOTFOUND) {
      ASSERT_GT(iterator_base->current->docId, skipToId);
      ASSERT_GT(iterator_base->lastDocId, skipToId);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else {
      ASSERT_EQ(rc, ITERATOR_EOF);
      ASSERT_TRUE(iterator_base->atEOF);
    }
  }
}


TEST_P(NotIteratorCommonTest, SkipToWCIds) {
  if (optimized && opt_max_doc_id.has_value()) {
    GTEST_SKIP() << "For optimized version the maxDocId is not necessarily respected (smaller than the max of Child or WCDocId): Is it worth adding this check? return base->lastDocId < ni->maxDocId ? ITERATOR_EOF : ITERATOR_OK; ";
  }
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  // Test skipping from 0
  for (t_docId id : wcDocIds) {
    t_docId expectedId;
    for (auto rsId: resultSet) {
      if (rsId >= id) {
        expectedId = rsId;
        break;
      }
    }
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    if (rc == ITERATOR_NOTFOUND) {
      ASSERT_GT(iterator_base->current->docId, id);
      ASSERT_GT(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else if (rc == ITERATOR_OK) {
      ASSERT_EQ(id, expectedId);
      ASSERT_EQ(iterator_base->current->docId, id);
      ASSERT_EQ(iterator_base->lastDocId, id);
    } else {
      ASSERT_EQ(rc, ITERATOR_EOF);
      ASSERT_TRUE(iterator_base->atEOF);
    }
  }

  // Test skipping from intermediate results
  iterator_base->Rewind(iterator_base);
  for (t_docId id : wcDocIds) {
    if (iterator_base->atEOF) {
      break;
    }
    t_docId skipToId;
    if (id > iterator_base->lastDocId) {
      skipToId = id;
    } else {
      for (auto wcid : wcDocIds) {
        if (wcid > iterator_base->lastDocId) {
          skipToId = wcid;
          break;
        }
      }
    }
    t_docId expectedId;
    for (auto rsId: resultSet) {
      if (rsId >= skipToId) {
        expectedId = rsId;
        break;
      }
    }

    NotIterator *ni = (NotIterator*) iterator_base;
    if (skipToId <= iterator_base->lastDocId) {
      break;
    }
    rc = iterator_base->SkipTo(iterator_base, skipToId);
    if (rc == ITERATOR_NOTFOUND) {
      ASSERT_GT(iterator_base->current->docId, skipToId);
      ASSERT_GT(iterator_base->lastDocId, skipToId);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else if (rc == ITERATOR_OK) {
      ASSERT_EQ(skipToId, expectedId);
      ASSERT_EQ(iterator_base->current->docId, skipToId);
      ASSERT_EQ(iterator_base->lastDocId, skipToId);
    } else {
      ASSERT_EQ(rc, ITERATOR_EOF);
      ASSERT_TRUE(iterator_base->atEOF);
    }
  }
}


TEST_P(NotIteratorCommonTest, SkipToAll) {
  if (optimized && opt_max_doc_id.has_value()) {
    GTEST_SKIP() << "For optimized version the maxDocId is not necessarily respected (smaller than the max of Child or WCDocId): Is it worth adding this check? return base->lastDocId < ni->maxDocId ? ITERATOR_EOF : ITERATOR_OK; ";
  }
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  for (t_docId id = 1; id < maxDocId; id++) {
    t_docId expectedId = 0;
    t_docId return_ok = false;
    for (auto rsId: resultSet) {
      if (rsId == id) {
        expectedId = rsId;
        return_ok = true;
        break;
      } else if (rsId > id) {
        expectedId = rsId;
        return_ok = false;
        break;
      }
    }

    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    if (rc == ITERATOR_NOTFOUND) {
      ASSERT_FALSE(return_ok) << "Expected NOTFOUND for id: " << id;
      ASSERT_GT(iterator_base->current->docId, id);
      ASSERT_GT(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else if (rc == ITERATOR_OK) {
      ASSERT_TRUE(return_ok) << "Expected OK for id: " << id;
      ASSERT_EQ(id, expectedId);
      ASSERT_EQ(iterator_base->current->docId, id);
      ASSERT_EQ(iterator_base->lastDocId, id);
    } else {
      ASSERT_EQ(expectedId, 0);
      ASSERT_EQ(rc, ITERATOR_EOF);
      ASSERT_TRUE(iterator_base->atEOF);
    }
  }
}

TEST_P(NotIteratorCommonTest, NumEstimated) {
  NotIterator *ni = (NotIterator *)iterator_base;
  if (optimized) {
    ASSERT_EQ(iterator_base->NumEstimated(iterator_base), ni->wcii->NumEstimated(ni->wcii));
  } else {
    ASSERT_EQ(iterator_base->NumEstimated(iterator_base), maxDocId);
  }
}

TEST_P(NotIteratorCommonTest, Rewind) {
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j <= i && j < resultSet.size(); j++) {
      ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
      ASSERT_EQ(iterator_base->current->docId, resultSet[j]);
      ASSERT_EQ(iterator_base->lastDocId, resultSet[j]);
    }
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->lastDocId, 0);
    ASSERT_FALSE(iterator_base->atEOF);
  }
}

// Parameters for the tests - just the child iterator document IDs
INSTANTIATE_TEST_SUITE_P(
  NotIteratorP,
  NotIteratorCommonTest,
  ::testing::Combine(
    ::testing::Values(
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // continuous
      std::vector<t_docId>{500, 600, 700, 800, 900, 1000}, // sparse and first large
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<t_docId> dist(1, 10000);
        for (int i = 0; i < 10000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }()
    ),
    ::testing::Values(
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // continuous
      std::vector<t_docId>{500, 600, 700, 800, 900, 1000}, // sparse and first large
      std::vector<t_docId>{}, // empty
      // Add long random list for wildcard IDs
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(43); // Different seed
        std::uniform_int_distribution<t_docId> dist(1, 20000);
        for (int i = 0; i < 10000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }()
    ),
    ::testing::Values(std::nullopt, 100),
    ::testing::Bool()
  )
);

class NotIteratorChildTimeoutTest : public NotIteratorCommonTest {
  protected:
  void TimeoutChildTestFirstRead() {
    NotIterator *ni = (NotIterator *)iterator_base;
    auto child = reinterpret_cast<MockIterator *>(ni->child);
    child->whenDone = ITERATOR_TIMEOUT;
    child->docIds.clear();
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }

  void TimeoutChildTestSubsequentRead() {
    NotIterator *ni = (NotIterator *)iterator_base;
    auto child = reinterpret_cast<MockIterator *>(ni->child);
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    if (ni->base.lastDocId < ni->child->NumEstimated(ni->child)) {
      child->whenDone = ITERATOR_TIMEOUT;
      child->docIds.clear();
      while (rc == ITERATOR_OK) {
        rc = iterator_base->Read(iterator_base);
      }
      ASSERT_EQ(rc, ITERATOR_TIMEOUT);
    }
  }

  void TimeoutChildTestSkipTo() {
    NotIterator *ni = (NotIterator *)iterator_base;
    auto child = reinterpret_cast<MockIterator *>(ni->child);
    child->whenDone = ITERATOR_TIMEOUT;
    child->docIds.clear();
    t_docId next = 1;
    IteratorStatus rc = ITERATOR_OK;
    while (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND) {
      rc = iterator_base->SkipTo(iterator_base, ++next);
    }
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }
};

TEST_P(NotIteratorChildTimeoutTest, TimeoutChildTestFirstRead) {
  TimeoutChildTestFirstRead();
}

TEST_P(NotIteratorChildTimeoutTest, TimeoutChildTestSubsequentRead) {
  TimeoutChildTestSubsequentRead();
}

TEST_P(NotIteratorChildTimeoutTest, TimeOutChildSkipTo) {
  TimeoutChildTestSkipTo();
}

INSTANTIATE_TEST_SUITE_P(
  NotIteratorChildTimeoutP,
  NotIteratorChildTimeoutTest,
  ::testing::Combine(
    ::testing::Values(
      std::vector<t_docId>{2, 4, 6, 8, 10},
      std::vector<t_docId>{5, 10, 15, 20, 25, 30},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150},
      std::vector<t_docId>{1, 2, 3, 6, 10, 15},
      std::vector<t_docId>{500, 600, 700, 800, 900, 1000}
    ),
    ::testing::Values(
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150, 1000, 2000},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15, 1000, 2000},
      std::vector<t_docId>{3, 4, 9, 25, 1000, 2000},
      std::vector<t_docId>{50, 60, 70, 80, 90, 100, 600, 750, 950, 1200}
    ),
    ::testing::Values(std::nullopt),
    ::testing::Bool()
  )
);

class NotIteratorSelfTimeoutTest : public NotIteratorCommonTest {
  protected:
  // Add member variable to store the context
  std::unique_ptr<MockQueryEvalCtx> mockQctx;

  void SetUp() override {
    // Get child document IDs from parameter
    std::tie(childDocIds, wcDocIds, opt_max_doc_id, optimized) = GetParam();

    // Find the maximum document ID
    if (opt_max_doc_id.has_value()) {
      maxDocId = opt_max_doc_id.value();
    } else {
      maxDocId = *std::max_element(childDocIds.begin(), childDocIds.end());
      if (optimized) {
        for (auto id : wcDocIds) {
          maxDocId = std::max(maxDocId, id);
        }
      }
      maxDocId += 5; // Add some buffer
    }
    // Compute resultSet from maxDocId and childDocIds
    resultSet.clear();
    for (t_docId id = 1; id <= maxDocId; id++) {
      if (std::find(childDocIds.begin(), childDocIds.end(), id) == childDocIds.end()) {
        resultSet.push_back(id);
      }
    }

    // Create a MockIterator with sleep time
    MockIterator* mockIter = new MockIterator(std::chrono::nanoseconds(200));
    mockIter->docIds = childDocIds;
    auto child = (QueryIterator*)mockIter;

    // Define timeout only once
    struct timespec timeout = {0, 1};
    if (optimized) {
      std::vector<t_docId> wildcard = {1, 2, 3};
      mockQctx = std::make_unique<MockQueryEvalCtx>(wildcard);
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, &mockQctx->qctx);
    } else {
      mockQctx = std::make_unique<MockQueryEvalCtx>(maxDocId, maxDocId);
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, &mockQctx->qctx);
    }
  }

  void TimeoutSelfTestRead() {
    NotIterator *ni = (NotIterator *)iterator_base;
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }

  void TimeoutSelfTestSkipTo() {
    NotIterator *ni = (NotIterator *)iterator_base;
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, 1);
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }
};

TEST_P(NotIteratorSelfTimeoutTest, TimeOutSelfRead) {
  GTEST_SKIP() << "Skipping this as Timer is not activated";
  TimeoutSelfTestRead();
}

TEST_P(NotIteratorSelfTimeoutTest, TimeOutSelfSkipTo) {
  GTEST_SKIP() << "Skipping this as Timer is not activated";
  TimeoutSelfTestSkipTo();
}

// Trying to create the case where the Child and WCII both have the same logic, so when Reading the first or Skipping to 1, it will do
// a big loop that will allow to trigger the timeout (5000 times it needs to check before verifying the timeout)
INSTANTIATE_TEST_SUITE_P(
  NotIteratorSelfTimeoutP,
  NotIteratorSelfTimeoutTest,
  ::testing::Combine(
    ::testing::Values(
      []() {
        std::vector<t_docId> ids;
        for (int i = 0; i < 5500; i++) {
          ids.push_back(i + 1);
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }()
    ),
    ::testing::Values(
      []() {
        std::vector<t_docId> ids;
        for (int i = 0; i < 5500; i++) {
          ids.push_back(i + 1);
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }()
    ),
    ::testing::Values(std::nullopt),
    ::testing::Bool()
  )
);

class NotIteratorNoChildTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  t_docId maxDocId = 50;
  std::unique_ptr<MockQueryEvalCtx> mockQctx;

  void SetUp() override {
    struct timespec timeout = {LONG_MAX, 999999999}; // Initialize with "infinite" timeout
    mockQctx = std::make_unique<MockQueryEvalCtx>(maxDocId, maxDocId);
    iterator_base = IT_V2(NewNotIterator)(nullptr, maxDocId, 1.0, timeout, &mockQctx->qctx);
  }

  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_F(NotIteratorNoChildTest, Read) {
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;

  // Test reading until EOF
  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
    ASSERT_EQ(ni->base.current->docId, i + 1);
    ASSERT_FALSE(ni->base.atEOF);
    i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ni->base.atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(i, maxDocId) << "Expected to read " << maxDocId << " documents";
}

TEST_F(NotIteratorNoChildTest, SkipTo) {
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  for (t_docId id = 1; id <= maxDocId; id++) {
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
  }

  // Test skipping beyond maxID returns EOF
  iterator_base->Rewind(iterator_base);
  rc = iterator_base->SkipTo(iterator_base, maxDocId + 1);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(NotIteratorNoChildTest, Rewind) {
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;
  for (int i = 0; i < maxDocId; i++) {
    for (int j = 0; j <= i && j < 5; j++) {
      ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
      ASSERT_EQ(iterator_base->current->docId, j + 1);
      ASSERT_EQ(iterator_base->lastDocId, j + 1);
    }
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->lastDocId, 0);
    ASSERT_FALSE(iterator_base->atEOF);
  }
}

class NotIteratorReducerTest : public ::testing::Test {};

TEST_F(NotIteratorReducerTest, TestNotWithNullChild) {
  // Test rule 1: If the child is NULL, return a wildcard iterator
  struct timespec timeout = {LONG_MAX, 999999999};
  t_docId maxDocId = 100;

  MockQueryEvalCtx mockQctx(maxDocId, maxDocId);

  QueryIterator *it = IT_V2(NewNotIterator)(nullptr, maxDocId, 1.0, timeout, &mockQctx.qctx);

  // Should return a wildcard iterator
  ASSERT_EQ(it->type, WILDCARD_ITERATOR);
  it->Free(it);
}

TEST_F(NotIteratorReducerTest, TestNotWithEmptyChild) {
  // Test rule 1: If the child is an empty iterator, return a wildcard iterator
  struct timespec timeout = {LONG_MAX, 999999999};
  t_docId maxDocId = 100;

  MockQueryEvalCtx mockQctx(maxDocId, maxDocId);

  QueryIterator *emptyChild = IT_V2(NewEmptyIterator)();
  QueryIterator *it = IT_V2(NewNotIterator)(emptyChild, maxDocId, 1.0, timeout, &mockQctx.qctx);

  // Should return a wildcard iterator
  ASSERT_EQ(it->type, WILDCARD_ITERATOR);
  it->Free(it);
}

TEST_F(NotIteratorReducerTest, TestNotWithEmptyChildOptimized) {
  // Test rule 1: If the child is an empty iterator, return a wildcard iterator
  struct timespec timeout = {LONG_MAX, 999999999};
  t_docId maxDocId = 100;

  std::vector<t_docId> wildcard = {1, 2, 3};
  MockQueryEvalCtx mockQctx(wildcard);

  QueryIterator *emptyChild = IT_V2(NewEmptyIterator)();
  QueryIterator *it = IT_V2(NewNotIterator)(emptyChild, maxDocId, 1.0, timeout, &mockQctx.qctx);

  // Should return a wildcard iterator
  ASSERT_EQ(it->type, READ_ITERATOR);
  it->Free(it);
}

TEST_F(NotIteratorReducerTest, TestNotWithWildcardChild) {
  // Test rule 2: If the child is a wildcard iterator, return an empty iterator
  struct timespec timeout = {LONG_MAX, 999999999};
  t_docId maxDocId = 100;

  std::vector<t_docId> wildcard = {1, 2, 3};
  MockQueryEvalCtx mockQctx(wildcard);

  QueryIterator *wildcardChild = IT_V2(NewWildcardIterator_NonOptimized)(maxDocId, maxDocId, 1.0);
  QueryIterator *it = IT_V2(NewNotIterator)(wildcardChild, maxDocId, 1.0, timeout, &mockQctx.qctx);

  // Should return an empty iterator
  ASSERT_EQ(it->type, EMPTY_ITERATOR);
  it->Free(it);
}

TEST_F(NotIteratorReducerTest, TestNotWithReaderWildcardChild) {
  // Test rule 2: If the child is a wildcard iterator, return an empty iterator
  struct timespec timeout = {LONG_MAX, 999999999};
  t_docId maxDocId = 100;
  size_t memsize;
  InvertedIndex *idx = NewInvertedIndex(static_cast<IndexFlags>(INDEX_DEFAULT_FLAGS), 1, &memsize);
  ASSERT_TRUE(idx != nullptr);
  ASSERT_TRUE(InvertedIndex_GetDecoder(idx->flags).seeker != nullptr);
  auto encoder = InvertedIndex_GetEncoder(idx->flags);
  for (t_docId i = 1; i < 1000; ++i) {
    auto res = (RSIndexResult) {
      .docId = i,
      .fieldMask = 1,
      .freq = 1,
      .type = RSResultType::RSResultType_Term,
    };
    InvertedIndex_WriteEntryGeneric(idx, encoder, i, &res);
  }
  // Create an iterator that reads only entries with field mask 2
  QueryIterator *wildcardChild = NewInvIndIterator_TermQuery(idx, nullptr, {.isFieldMask = true, .value = {.mask = 2}}, nullptr, 1.0);
  InvIndIterator* invIdxIt = (InvIndIterator *)wildcardChild;
  invIdxIt->isWildcard = true;
  MockQueryEvalCtx mockQctx(maxDocId, maxDocId);
  QueryIterator *it = IT_V2(NewNotIterator)(wildcardChild, maxDocId, 1.0, timeout, &mockQctx.qctx);

  // Should return an empty iterator
  ASSERT_EQ(it->type, EMPTY_ITERATOR);
  it->Free(it);
  InvertedIndex_Free(idx);
}
