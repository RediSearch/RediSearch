/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rmutil/alloc.h"

#include "gtest/gtest.h"
#include "iterator_util.h"

#include "src/iterators/not_iterator.h"

class NotIteratorCommonTest : public ::testing::TestWithParam<std::tuple<std::vector<t_docId>, bool>> {
protected:
    std::vector<t_docId> childDocIds;
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
      std::tie(childDocIds, optimized) = GetParam();

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

      auto child = (QueryIterator *) new MockIterator(childDocIds);


      struct timespec timeout = {LONG_MAX, 999999999}; // Initialize with "infinite" timeout
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

    // Test reading until EOF
    size_t i = 0;
    size_t not_results = 0;
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

TEST_P(NotIteratorCommonTest, SkipTo) {
  NotIterator *ni = (NotIterator *)iterator_base;
  IteratorStatus rc;

  for (t_docId id : childDocIds) {
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_NOTFOUND);
  }

  for (t_docId id = 1; id <= maxDocId; id++) {
    if (std::find(childDocIds.begin(), childDocIds.end(), id) != childDocIds.end()) {
      continue;
    }
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
      std::vector<t_docId>{2, 4, 6, 8, 10},
      std::vector<t_docId>{5, 10, 15, 20, 25, 30},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150}
    ),
    ::testing::Values(false, true)
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
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150}
    ),
    ::testing::Values(false, true)
  )
);

class NotIteratorWildCardTimeoutTest : public NotIteratorCommonTest {
  protected:
  void TimeoutWildCardTestRead() {
    if (!optimized) return;
    NotIterator *ni = (NotIterator *)iterator_base;
    if (ni->wcii) {
      ni->wcii->Free(ni->wcii);
      MockIterator *mock_wcii = new MockIterator(std::vector<t_docId>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
      ni->wcii = (QueryIterator*)mock_wcii;
    }
    IteratorStatus rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);

    auto wcii = reinterpret_cast<MockIterator *>(ni->wcii);
    wcii->whenDone = ITERATOR_TIMEOUT;
    wcii->docIds.clear();
    while (rc == ITERATOR_OK) {
      rc = iterator_base->Read(iterator_base);
    }
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }

  void TimeoutWildCardTestSkipTo() {
    if (!optimized) return;
    NotIterator *ni = (NotIterator *)iterator_base;
    if (ni->wcii) {
      ni->wcii->Free(ni->wcii);
      MockIterator *mock_wcii = new MockIterator(std::vector<t_docId>{});
      ni->wcii = (QueryIterator*)mock_wcii;
      mock_wcii->whenDone = ITERATOR_TIMEOUT;
      mock_wcii->docIds.clear();
    }
    t_docId next = 1;
    IteratorStatus rc = ITERATOR_OK;
    while (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND) {
      rc = iterator_base->SkipTo(iterator_base, ++next);
    }
    ASSERT_EQ(rc, ITERATOR_TIMEOUT);
  }
};

TEST_P(NotIteratorWildCardTimeoutTest, TimeOutChildRead) {
  TimeoutWildCardTestRead();
}

TEST_P(NotIteratorWildCardTimeoutTest, TimeOutWildCardSkipTo) {
  TimeoutWildCardTestSkipTo();
}

INSTANTIATE_TEST_SUITE_P(
  NotIteratorWildCardTimeoutP,
  NotIteratorWildCardTimeoutTest,
  ::testing::Combine(
    ::testing::Values(
      std::vector<t_docId>{2, 4, 6, 8, 10},
      std::vector<t_docId>{5, 10, 15, 20, 25, 30},
      std::vector<t_docId>{1, 3, 5, 7, 9, 11, 13, 15},
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150}
    ),
    ::testing::Values(false, true)
  )
);

class NotIteratorSelfTimeoutTest : public NotIteratorCommonTest {
  protected:
  void SetUp() override {
    // Get child document IDs from parameter
    std::tie(childDocIds, optimized) = GetParam();

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

    struct timespec timeout = {0, 100};
        // Create a MockIterator with sleep time
    MockIterator* mockIter = new MockIterator(std::chrono::milliseconds(100));
    mockIter->docIds = childDocIds;
    auto child = (QueryIterator*)mockIter;

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
    } else {
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, nullptr);
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
      std::vector<t_docId>{1, 2, 3, 4, 5, 6, 100, 150}
    ),
    ::testing::Values(false, true)
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
  size_t not_results = 0;
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
