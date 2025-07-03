/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <algorithm>
#include <vector>
#include "rmutil/alloc.h"
#include "gtest/gtest.h"

#include "src/iterators/optional_iterator.h"
#include "src/iterators/idlist_iterator.h"
#include "src/iterators/empty_iterator.h"
#include "iterator_util.h"
#include "index_utils.h"
#include "src/iterators/wildcard_iterator.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/inverted_index/inverted_index.h"


// Test optional iterator
class OptionalIteratorTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  std::vector<t_docId> childDocIds;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;
  const double weight = 2.0;

  void SetUp() override {
    // Create child iterator with specific docIds
    childDocIds = {10, 20, 30, 50, 80};
    QueryIterator *child = (QueryIterator *)new MockIterator(childDocIds);

    // Create optional iterator with child
    MockQueryEvalCtx ctx(maxDocId, numDocs);
    iterator_base = IT_V2(NewOptionalIterator)(child, &ctx.qctx, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
  }
};

TEST_F(OptionalIteratorTest, ReadMixedResults) {
  // Test reading - should return mix of real and virtual results

  EXPECT_EQ(iterator_base->NumEstimated(iterator_base), maxDocId);

  for (t_docId i = 1; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_NE(iterator_base->current, nullptr);
    ASSERT_EQ(iterator_base->current->docId, i);
    ASSERT_EQ(iterator_base->lastDocId, i);

    // Check if this is a real hit from child or virtual
    bool isRealHit = std::find(childDocIds.begin(), childDocIds.end(), i) != childDocIds.end();
    OptionalIterator *oi = (OptionalIterator *)iterator_base;

    if (isRealHit) {
      // Real hit should have the weight applied
      ASSERT_EQ(iterator_base->current->weight, weight);
      ASSERT_EQ(iterator_base->current, oi->child->current);
    } else {
      // Virtual hit
      ASSERT_EQ(iterator_base->current, oi->virt);
      ASSERT_EQ(iterator_base->current->freq, 1);
      ASSERT_EQ(iterator_base->current->fieldMask, RS_FIELDMASK_ALL);
    }
  }

  // After reading all docs, should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(OptionalIteratorTest, SkipToRealHit) {
  // Skip to a docId that exists in child
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 20), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 20);
  ASSERT_EQ(iterator_base->current->weight, weight);
  ASSERT_EQ(iterator_base->lastDocId, 20);

  // Should be real hit from child
  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(iterator_base->current, oi->child->current);
}

TEST_F(OptionalIteratorTest, SkipToVirtualHit) {
  // Skip to a docId that doesn't exist in child
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 25), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 25);
  ASSERT_EQ(iterator_base->lastDocId, 25);

  // Should be virtual hit
  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(iterator_base->current, oi->virt);
}

TEST_F(OptionalIteratorTest, SkipToSequence) {
  // Test skipping to various docIds in sequence
  t_docId targets[] = {5, 15, 25, 35, 45, 55, 65, 75, 85, 95};

  for (auto &target : targets) {
    ASSERT_EQ(iterator_base->SkipTo(iterator_base, target), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, target);
    ASSERT_EQ(iterator_base->lastDocId, target);

    // Check if it's a real or virtual hit
    bool isRealHit = std::find(childDocIds.begin(), childDocIds.end(), target) != childDocIds.end();
    OptionalIterator *oi = (OptionalIterator *)iterator_base;

    if (isRealHit) {
      ASSERT_EQ(iterator_base->current, oi->child->current);
      ASSERT_EQ(iterator_base->current->weight, weight);
    } else {
      ASSERT_EQ(iterator_base->current, oi->virt);
    }
  }
}

TEST_F(OptionalIteratorTest, RewindBehavior) {
  // Read some documents first
  for (int i = 0; i < 10; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->lastDocId, 10);

  // Test that Rewind resets the iterator
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);

  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(oi->virt->docId, 0);

  // After Rewind, should be able to read from the beginning
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
}

TEST_F(OptionalIteratorTest, EOFBehavior) {
  // Test EOF when reaching maxDocId
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, maxDocId);
  ASSERT_EQ(iterator_base->lastDocId, maxDocId);

  // Next read should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  // Further operations should still return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
}

TEST_F(OptionalIteratorTest, WeightApplication) {
  // Test that weight is correctly applied to real hits
  for (t_docId docId : childDocIds) {
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->SkipTo(iterator_base, docId), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, docId);
    ASSERT_EQ(iterator_base->current->weight, weight);

    // Verify it's a real hit from child
    OptionalIterator *oi = (OptionalIterator *)iterator_base;
    ASSERT_EQ(iterator_base->current, oi->child->current);
  }
}

TEST_F(OptionalIteratorTest, VirtualResultWeight) {
  // Test that virtual results have the correct weight
  OptionalIterator *oi = (OptionalIterator *)iterator_base;

  // Skip to a virtual hit (not in childDocIds)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 15), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current, oi->virt);
  ASSERT_EQ(iterator_base->current->weight, weight);
}

// Test timeout scenarios
class OptionalIteratorTimeoutTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;
  const double weight = 2.0;

  void SetUp() override {
    // Create child iterator that returns ITERATOR_TIMEOUT after exhausting its documents
    QueryIterator *child = (QueryIterator *)new MockIterator(ITERATOR_TIMEOUT, 10UL, 20UL, 30UL);

    // Create optional iterator with timeout child
    MockQueryEvalCtx ctx(maxDocId, numDocs);
    iterator_base = IT_V2(NewOptionalIterator)(child, &ctx.qctx, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
  }
};

TEST_F(OptionalIteratorTimeoutTest, ReadTimeoutFromChild) {
  OptionalIterator *oi = (OptionalIterator *)iterator_base;

  // Read the real hits first (10, 20, 30)
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
  ASSERT_EQ(iterator_base->current, oi->virt); // Virtual hit

  // Continue reading - should get virtual hits until we reach child's documents
  for (t_docId i = 2; i <= 9; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
    // Should be virtual hits
    ASSERT_EQ(iterator_base->current, oi->virt);
  }

  // Read docId 10 - should be a real hit
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 10);
  ASSERT_EQ(iterator_base->current, oi->child->current);

  // Continue reading virtual hits
  for (t_docId i = 11; i <= 19; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
    ASSERT_EQ(iterator_base->current, oi->virt);
  }

  // Read docId 20 - should be a real hit
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 20);
  ASSERT_EQ(iterator_base->current, oi->child->current);

  // Continue reading virtual hits
  for (t_docId i = 21; i <= 29; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
    ASSERT_EQ(iterator_base->current, oi->virt);
  }

  // Read docId 30 - should be a real hit
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 30);
  ASSERT_EQ(iterator_base->current, oi->child->current);

  // Now the child iterator is exhausted, next read should trigger timeout
  // when the optional iterator tries to advance the child beyond its documents
  IteratorStatus rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_TIMEOUT); // Should timeout when child times out
}

TEST_F(OptionalIteratorTimeoutTest, SkipToTimeoutFromChild) {
  OptionalIterator *oi = (OptionalIterator *)iterator_base;

  // Skip to a document that exists in child (should work)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 20), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 20);
  ASSERT_EQ(iterator_base->current, oi->child->current);

  // Skip to a document beyond child's range
  // This should trigger timeout when trying to advance the child
  IteratorStatus rc = iterator_base->SkipTo(iterator_base, 50);
  ASSERT_EQ(rc, ITERATOR_TIMEOUT); // Should timeout when child times out
}

TEST_F(OptionalIteratorTimeoutTest, RewindAfterTimeout) {
  // Read past the child's documents to trigger timeout handling
  for (int i = 0; i < 35; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->lastDocId, 30);

  // Rewind should reset everything
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);

  // Should be able to read from beginning again
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
}


// Test optional iterator with empty child iterator (all virtual hits)
class OptionalIteratorWithEmptyChildTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  QueryIterator *empty_child;
  const t_docId maxDocId = 50;
  const size_t numDocs = 25;
  const double weight = 3.0;

  void SetUp() override {
    // Create optional iterator with empty child (no real hits, all virtual)
    empty_child = IT_V2(NewEmptyIterator)();
    MockQueryEvalCtx ctx(maxDocId, numDocs);
    iterator_base = IT_V2(NewOptionalIterator)(empty_child, &ctx.qctx, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
    // Note: empty_child is freed by iterator_base->Free(), so don't free it separately
  }
};

TEST_F(OptionalIteratorWithEmptyChildTest, ReadAllVirtualResults) {
  // Test reading - should return all virtual results
  for (t_docId i = 1; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_NE(iterator_base->current, nullptr);
    ASSERT_EQ(iterator_base->current->docId, i);
    ASSERT_EQ(iterator_base->lastDocId, i);

    // All hits should be virtual
    ASSERT_EQ(iterator_base->current->type, RSResultType_Virtual);
    ASSERT_EQ(iterator_base->current->weight, weight);
    ASSERT_EQ(iterator_base->current->freq, 1);
    ASSERT_EQ(iterator_base->current->fieldMask, RS_FIELDMASK_ALL);
  }

  // After reading all docs, should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(OptionalIteratorWithEmptyChildTest, SkipToVirtualHits) {
  // Skip to various docIds - all should be virtual hits
  t_docId targets[] = {5, 15, 25, 35, 45};

  for (auto &target : targets) {
    ASSERT_EQ(iterator_base->SkipTo(iterator_base, target), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, target);
    ASSERT_EQ(iterator_base->lastDocId, target);

    // Should be virtual hit
    ASSERT_EQ(iterator_base->current->type, RSResultType_Virtual);
    ASSERT_EQ(iterator_base->current->weight, weight);
  }
}

TEST_F(OptionalIteratorWithEmptyChildTest, RewindBehavior) {
  // Read some documents first
  for (int i = 0; i < 10; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->lastDocId, 10);

  // Test that Rewind resets the iterator
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);

  // After Rewind, should be able to read from the beginning
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
  ASSERT_EQ(iterator_base->current->type, RSResultType_Virtual);
}

TEST_F(OptionalIteratorWithEmptyChildTest, EOFBehavior) {
  // Test EOF when reaching maxDocId
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, maxDocId);
  ASSERT_EQ(iterator_base->lastDocId, maxDocId);

  // Should be virtual hit
  ASSERT_EQ(iterator_base->current->type, RSResultType_Virtual);

  // Next read should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  // Further operations should still return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
}

TEST_F(OptionalIteratorWithEmptyChildTest, VirtualResultProperties) {
  // Test that virtual results have correct properties
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);

  ASSERT_EQ(iterator_base->current->type, RSResultType_Virtual);
  ASSERT_EQ(iterator_base->current->docId, 1);
  ASSERT_EQ(iterator_base->current->weight, weight);
  ASSERT_EQ(iterator_base->current->freq, 1);
  ASSERT_EQ(iterator_base->current->fieldMask, RS_FIELDMASK_ALL);
}


class OptionalIteratorOptimized : public ::testing::TestWithParam<std::tuple<bool, bool>> {
protected:
  QueryIterator *iterator;
  std::vector<t_docId> childDocIds;
  std::vector<t_docId> wildcardDocIds;
  MockQueryEvalCtx *q;

  void SetUp() override {
    auto [firstFromChild, lastFromChild] = GetParam();
    // Create child iterator with specific docIds
    childDocIds = {20, 30, 40, 50, 60, 70, 80, 90};
    if (firstFromChild) {
      childDocIds.insert(childDocIds.begin(), 10); // Add first docId
    }
    if (lastFromChild) {
      childDocIds.push_back(100); // Add last docId
    }
    QueryIterator *child = (QueryIterator *)new MockIterator(childDocIds);

    // Create wildcard iterator with specific docIds
    for (t_docId i = 5; i <= 95; i += 5) {
      wildcardDocIds.push_back(i);
    }

    // Create optional iterator with child and wildcard
    q = new MockQueryEvalCtx(wildcardDocIds);
    if (lastFromChild) {
      q->docTable.maxDocId = childDocIds.back(); // Ensure maxDocId is set to include last child doc
    }
    iterator = IT_V2(NewOptionalIterator)(child, &q->qctx, 4.6);
  }

  void TearDown() override {
    if (iterator) {
      iterator->Free(iterator);
    }
    delete q;
  }
};

INSTANTIATE_TEST_SUITE_P(OptionalIteratorOptimizedTests, OptionalIteratorOptimized,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));

TEST_P(OptionalIteratorOptimized, Read) {
  OptionalIterator *oi = (OptionalIterator *)iterator;

  EXPECT_EQ(iterator->NumEstimated(iterator), wildcardDocIds.size());

  // Read all documents
  for (auto &id : wildcardDocIds) {
    IteratorStatus status = iterator->Read(iterator);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iterator->lastDocId, id);
    ASSERT_EQ(iterator->current->docId, id);
    ASSERT_EQ(iterator->current->weight, 4.6);

    if (std::find(childDocIds.begin(), childDocIds.end(), id) != childDocIds.end()) {
      // Should be a real hit from child
      ASSERT_EQ(iterator->current, oi->child->current);
    } else {
      // Should be a virtual hit
      ASSERT_EQ(iterator->current, oi->virt);
    }
  }
  // Read should return EOF after all wildcard docs
  IteratorStatus status = iterator->Read(iterator);
  ASSERT_EQ(status, ITERATOR_EOF);
  ASSERT_TRUE(iterator->atEOF);
  ASSERT_EQ(iterator->lastDocId, wildcardDocIds.back());
  status = iterator->Read(iterator); // Should return EOF again
  ASSERT_EQ(status, ITERATOR_EOF);
  ASSERT_TRUE(iterator->atEOF);
  ASSERT_EQ(iterator->lastDocId, wildcardDocIds.back());
}

TEST_P(OptionalIteratorOptimized, SkipTo) {
  OptionalIterator *oi = (OptionalIterator *)iterator;

  // Skip to all the ids in the wildcard range
  t_docId id = 1;
  for (const auto &nextValidId : wildcardDocIds) {
    while (id < nextValidId) {
      iterator->Rewind(iterator);
      IteratorStatus status = iterator->SkipTo(iterator, id);
      ASSERT_EQ(status, ITERATOR_NOTFOUND);
      ASSERT_EQ(iterator->lastDocId, nextValidId);
      ASSERT_EQ(iterator->current->docId, nextValidId);
      ASSERT_EQ(iterator->current->weight, 4.6);
      if (std::find(childDocIds.begin(), childDocIds.end(), nextValidId) != childDocIds.end()) {
        // Should be a real hit from child
        ASSERT_EQ(iterator->current, oi->child->current);
      } else {
        // Should be a virtual hit
        ASSERT_EQ(iterator->current, oi->virt);
      }
      id++;
    }
    iterator->Rewind(iterator);
    IteratorStatus status = iterator->SkipTo(iterator, nextValidId);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iterator->lastDocId, nextValidId);
    ASSERT_EQ(iterator->current->docId, nextValidId);
    ASSERT_EQ(iterator->current->weight, 4.6);
    if (std::find(childDocIds.begin(), childDocIds.end(), nextValidId) != childDocIds.end()) {
      // Should be a real hit from child
      ASSERT_EQ(iterator->current, oi->child->current);
    } else {
      // Should be a virtual hit
      ASSERT_EQ(iterator->current, oi->virt);
    }
    id++;
  }
  // After the last id, should return EOF
  IteratorStatus status = iterator->SkipTo(iterator, iterator->lastDocId + 1);
  ASSERT_EQ(status, ITERATOR_EOF);
  ASSERT_TRUE(iterator->atEOF);
  ASSERT_EQ(iterator->lastDocId, wildcardDocIds.back());
  status = iterator->SkipTo(iterator, iterator->lastDocId + 2); // Should return EOF again
  ASSERT_EQ(status, ITERATOR_EOF);
  ASSERT_TRUE(iterator->atEOF);
  ASSERT_EQ(iterator->lastDocId, wildcardDocIds.back());


  // Skip between any id in the wildcard range to any id
  for (const auto &id : wildcardDocIds) {
    for (t_docId skipToId = id + 1; skipToId <= wildcardDocIds.back(); skipToId++) {
      iterator->Rewind(iterator);
      IteratorStatus status = iterator->SkipTo(iterator, id);
      ASSERT_EQ(status, ITERATOR_OK);
      ASSERT_EQ(iterator->lastDocId, id);
      ASSERT_EQ(iterator->current->docId, id);
      ASSERT_EQ(iterator->current->weight, 4.6);

      auto nextValidId = *std::lower_bound(wildcardDocIds.begin(), wildcardDocIds.end(), skipToId);
      status = iterator->SkipTo(iterator, skipToId);
      ASSERT_EQ(iterator->lastDocId, nextValidId);
      if (nextValidId == skipToId) {
        ASSERT_EQ(status, ITERATOR_OK);
      } else {
        ASSERT_LT(skipToId, nextValidId);
        ASSERT_EQ(status, ITERATOR_NOTFOUND);
      }
      if (std::find(childDocIds.begin(), childDocIds.end(), nextValidId) != childDocIds.end()) {
        ASSERT_EQ(iterator->current, ((OptionalIterator *)iterator)->child->current);
      } else {
        ASSERT_EQ(iterator->current, ((OptionalIterator *)iterator)->virt);
      }
    }
  }
}

// Test OptionalIteratorReducer optimizations
class OptionalIteratorReducerTest : public ::testing::Test {};

TEST_F(OptionalIteratorReducerTest, TestOptionalWithNullChild) {
  // Test rule 1: If the child is NULL, return a wildcard iterator
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double weight = 2.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create optional iterator with NULL child
  QueryIterator *it = IT_V2(NewOptionalIterator)(nullptr, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, weight);
  ASSERT_EQ(it->current->type, RSResultType_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithEmptyChild) {
  // Test rule 1: If the child is an empty iterator, return a wildcard iterator
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double weight = 2.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create empty child iterator
  QueryIterator *emptyChild = IT_V2(NewEmptyIterator)();

  // Create optional iterator with empty child
  QueryIterator *it = IT_V2(NewOptionalIterator)(emptyChild, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, weight);
  ASSERT_EQ(it->current->type, RSResultType_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithWildcardChild) {
  // Test rule 2: If the child is a wildcard iterator, return it directly
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double childWeight = 3.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create wildcard child iterator
  QueryIterator *wildcardChild = IT_V2(NewWildcardIterator_NonOptimized)(maxDocId, numDocs, childWeight);

  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = IT_V2(NewOptionalIterator)(wildcardChild, &ctx.qctx, 2.0);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);
  ASSERT_EQ(it, wildcardChild);

  // Read first document and check properties - should have child's weight
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, childWeight);
  ASSERT_EQ(it->current->type, RSResultType_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithReaderWildcardChild) {
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double childWeight = 3.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);
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

  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = IT_V2(NewOptionalIterator)(wildcardChild, &ctx.qctx, 2.0);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == READ_ITERATOR);
  ASSERT_EQ(it, wildcardChild);
  it->Free(it);
  InvertedIndex_Free(idx);
}
