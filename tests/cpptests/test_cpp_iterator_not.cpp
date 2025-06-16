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

class NotIteratorCommonTest : public ::testing::TestWithParam<std::tuple<std::vector<t_docId>, std::vector<t_docId>, bool>> {
protected:
  std::vector<t_docId> childDocIds;
  std::vector<t_docId> wcDocIds;
  std::vector<t_docId> resultSet;
  t_docId maxDocId;
  QueryIterator *iterator_base;
  bool optimized;
  IndexSpec *spec = nullptr;
  RedisSearchCtx *sctx = nullptr;
  QueryEvalCtx *qctx = nullptr;
  DocTable *docTable = nullptr;

  void SetUp() override {
    // Get child document IDs from parameter
    std::tie(childDocIds, wcDocIds, optimized) = GetParam();
    // Consider wcDocIds only if optimized is true

    // Find the maximum document ID
    maxDocId = *std::max_element(childDocIds.begin(), childDocIds.end());
    if (optimized) {
      for (auto id : wcDocIds) {
        maxDocId = std::max(maxDocId, id);
      }
    }
    maxDocId += 5; // Add some buffer
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
          resultSet.push_back(wcId);
        }
      }
    }

    auto child = (QueryIterator *) new MockIterator(childDocIds);
    struct timespec timeout = {LONG_MAX, 999999999}; // Initialize with "infinite" timeout
    // Store the wildcard iterator in the NotIterator structure instead of directly in QueryIterator
    if (optimized) {
      spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
      spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
      spec->rule->index_all = true;

      sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
      sctx->spec = spec;

      docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
      docTable->maxDocId = maxDocId;
      docTable->size = maxDocId;

      qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
      qctx->sctx = sctx;
      qctx->docTable = docTable;

      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, qctx);
      NotIterator *ni = (NotIterator *)iterator_base;
      ni->wcii->Free(ni->wcii);
      ni->wcii = (QueryIterator *) new MockIterator(wcDocIds);
    } else {
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, nullptr);
    }
  }

  void TearDown() override {
    iterator_base->Free(iterator_base);
    if (spec && spec->rule) rm_free(spec->rule);
    if (spec) rm_free(spec);
    if (sctx) rm_free(sctx);
    if (docTable) rm_free(docTable);
    if (qctx) rm_free(qctx);
  }
};

TEST_P(NotIteratorCommonTest, Read) {
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
      ASSERT_FALSE(return_ok);
      ASSERT_GT(iterator_base->current->docId, id);
      ASSERT_GT(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, expectedId);
      ASSERT_EQ(iterator_base->lastDocId, expectedId);
    } else if (rc == ITERATOR_OK) {
      ASSERT_TRUE(return_ok);
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
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), maxDocId);
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
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
      std::vector<t_docId>{2, 4, 6, 8, 10},
      std::vector<t_docId>{5, 10, 15, 20, 25, 30},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150},
      std::vector<t_docId>{1, 2, 3, 6, 10, 15},
      std::vector<t_docId>{500, 600, 700, 800, 900, 1000},
      // Add long random list
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<t_docId> dist(1, 10000);
        for (int i = 0; i < 1000000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }(),
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<t_docId> dist(1, 10000);
        for (int i = 0; i < 50000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }(),
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
      std::vector<t_docId>{3, 4, 9},
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{3, 4, 9, 25},
      std::vector<t_docId>{50, 60, 70, 80, 90, 100, 600, 750, 950, 1200},
      std::vector<t_docId>{},
      // Add long random list for wildcard IDs
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(43); // Different seed
        std::uniform_int_distribution<t_docId> dist(1, 20000);
        for (int i = 0; i < 50000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }(),
      []() {
        std::vector<t_docId> ids;
        std::mt19937 gen(43); // Different seed
        std::uniform_int_distribution<t_docId> dist(1, 20000);
        for (int i = 0; i < 30000; i++) {
          ids.push_back(dist(gen));
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
      }(),
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
    ::testing::Values(false)
  )
);

class NotIteratorChildTimeoutTest : public NotIteratorCommonTest {
  protected:
  void TimeoutChildTestRead() {
    NotIterator *ni = (NotIterator *)iterator_base;
    auto child = reinterpret_cast<MockIterator *>(ni->child);
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    child->whenDone = ITERATOR_TIMEOUT;
    child->docIds.clear();
    while (rc == ITERATOR_OK) {
      rc = iterator_base->Read(iterator_base);
    }
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
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

TEST_P(NotIteratorChildTimeoutTest, TimeOutChildRead) {
  TimeoutChildTestRead();
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
    ::testing::Values(false)
  )
);

class NotIteratorSelfTimeoutTest : public NotIteratorCommonTest {
  protected:
  void SetUp() override {
    // Get child document IDs from parameter
    std::tie(childDocIds, wcDocIds, optimized) = GetParam();

    // Find the maximum document ID
    maxDocId = 0;
    for (auto id : childDocIds) {
      maxDocId = std::max(maxDocId, id);
    }
    maxDocId += 5; // Add some buffer
    // Compute resultSet from maxDocId and childDocIds
    resultSet.clear();
    for (t_docId id = 1; id <= maxDocId; id++) {
      if (std::find(childDocIds.begin(), childDocIds.end(), id) == childDocIds.end()) {
        resultSet.push_back(id);
      }
    }

    // Create a MockIterator with sleep time
    MockIterator* mockIter = new MockIterator(std::chrono::milliseconds(100));
    mockIter->docIds = childDocIds;
    auto child = (QueryIterator*)mockIter;

    // Define timeout only once
    struct timespec timeout = {0, 100};
    iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, nullptr);

    // Store the wildcard iterator in the NotIterator structure instead of directly in QueryIterator
    if (optimized) {
      NotIterator *ni = (NotIterator *)iterator_base;
      ni->wcii = (QueryIterator *) new MockIterator(wcDocIds);
    }
  }

  void TimeoutSelfTestRead() {
    NotIterator *ni = (NotIterator *)iterator_base;
    auto child = reinterpret_cast<MockIterator *>(ni->child);
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    child->whenDone = ITERATOR_TIMEOUT;
    child->docIds.clear();
    while (rc == ITERATOR_OK) {
      rc = iterator_base->Read(iterator_base);
    }
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }

  void TimeoutSelfTestSkipTo() {
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

TEST_P(NotIteratorSelfTimeoutTest, TimeOutSelfRead) {
  TimeoutSelfTestRead();
}

TEST_P(NotIteratorSelfTimeoutTest, TimeOutSelfSkipTo) {
  TimeoutSelfTestSkipTo();
}

INSTANTIATE_TEST_SUITE_P(
  NotIteratorSelfTimeoutP,
  NotIteratorSelfTimeoutTest,
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
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{3, 4, 9, 25},
      std::vector<t_docId>{50, 60, 70, 80, 90, 100, 600, 750, 950, 1200},
      std::vector<t_docId>{}
    ),
    ::testing::Values(false)
  )
);

class NotIteratorNoChildTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  t_docId maxDocId = 50;

  void SetUp() override {
    struct timespec timeout = {LONG_MAX, 999999999}; // Initialize with "infinite" timeout
    iterator_base = IT_V2(NewNotIterator)(nullptr, maxDocId, 1.0, timeout, nullptr);
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
