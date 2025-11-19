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
#include "inverted_index.h"


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
    iterator_base = NewOptionalIterator(child, &ctx.qctx, weight);
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
  ASSERT_EQ(iterator_base->current->weight, 0);
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
    iterator_base = NewOptionalIterator(child, &ctx.qctx, weight);
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
    empty_child = NewEmptyIterator();
    MockQueryEvalCtx ctx(maxDocId, numDocs);
    iterator_base = NewOptionalIterator(empty_child, &ctx.qctx, weight);
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
    ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Virtual);
    ASSERT_EQ(iterator_base->current->weight, 0);
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
    ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Virtual);
    ASSERT_EQ(iterator_base->current->weight, 0);
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
  ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Virtual);
}

TEST_F(OptionalIteratorWithEmptyChildTest, EOFBehavior) {
  // Test EOF when reaching maxDocId
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, maxDocId);
  ASSERT_EQ(iterator_base->lastDocId, maxDocId);

  // Should be virtual hit
  ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Virtual);

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

  ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Virtual);
  ASSERT_EQ(iterator_base->current->docId, 1);
  ASSERT_EQ(iterator_base->current->weight, 0);
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
      q->spec.docs.maxDocId = childDocIds.back(); // Ensure maxDocId is set to include last child doc
    }
    iterator = NewOptionalIterator(child, &q->qctx, 4.6);
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

    if (std::find(childDocIds.begin(), childDocIds.end(), id) != childDocIds.end()) {
      // Should be a real hit from child
      ASSERT_EQ(iterator->current, oi->child->current);
      ASSERT_EQ(iterator->current->weight, 4.6);
    } else {
      // Should be a virtual hit
      ASSERT_EQ(iterator->current, oi->virt);
      ASSERT_EQ(iterator->current->weight, 0);
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
      if (std::find(childDocIds.begin(), childDocIds.end(), nextValidId) != childDocIds.end()) {
        // Should be a real hit from child
        ASSERT_EQ(iterator->current, oi->child->current);
        ASSERT_EQ(iterator->current->weight, 4.6);
      } else {
        // Should be a virtual hit
        ASSERT_EQ(iterator->current, oi->virt);
        ASSERT_EQ(iterator->current->weight, 0);
      }
      id++;
    }
    iterator->Rewind(iterator);
    IteratorStatus status = iterator->SkipTo(iterator, nextValidId);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iterator->lastDocId, nextValidId);
    ASSERT_EQ(iterator->current->docId, nextValidId);
    if (std::find(childDocIds.begin(), childDocIds.end(), nextValidId) != childDocIds.end()) {
      // Should be a real hit from child
      ASSERT_EQ(iterator->current, oi->child->current);
      ASSERT_EQ(iterator->current->weight, 4.6);
    } else {
      // Should be a virtual hit
      ASSERT_EQ(iterator->current, oi->virt);
      ASSERT_EQ(iterator->current->weight, 0);
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
        ASSERT_EQ(iterator->current->weight, 4.6);
      } else {
        ASSERT_EQ(iterator->current, ((OptionalIterator *)iterator)->virt);
        ASSERT_EQ(iterator->current->weight, 0);
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
  QueryIterator *it = NewOptionalIterator(nullptr, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, 0);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

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
  QueryIterator *emptyChild = NewEmptyIterator();

  // Create optional iterator with empty child
  QueryIterator *it = NewOptionalIterator(emptyChild, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, 0);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

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
  QueryIterator *wildcardChild = NewWildcardIterator_NonOptimized(maxDocId, numDocs, 2.0);

  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = NewOptionalIterator(wildcardChild, &ctx.qctx, childWeight);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);
  ASSERT_EQ(it, wildcardChild);

  // Read first document and check properties - should have child's weight
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, childWeight);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithReaderWildcardChild) {
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double childWeight = 3.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);
  size_t memsize;
  InvertedIndex *idx = NewInvertedIndex(static_cast<IndexFlags>(INDEX_DEFAULT_FLAGS), &memsize);
  ASSERT_TRUE(idx != nullptr);
  for (t_docId i = 1; i < 1000; ++i) {
    auto res = (RSIndexResult) {
      .docId = i,
      .fieldMask = 1,
      .freq = 1,
      .data = {.term_tag = RSResultData_Tag::RSResultData_Term},
    };
    InvertedIndex_WriteEntryGeneric(idx, &res);
  }
  // Create an iterator that reads only entries with field mask 2
  QueryIterator *wildcardChild = NewInvIndIterator_TermQuery(idx, nullptr, {.mask_tag = FieldMaskOrIndex_Mask, .mask = 2}, nullptr, 1.0);
  InvIndIterator* invIdxIt = (InvIndIterator *)wildcardChild;
  invIdxIt->isWildcard = true;

  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = NewOptionalIterator(wildcardChild, &ctx.qctx, 2.0);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == INV_IDX_ITERATOR);
  ASSERT_EQ(it, wildcardChild);
  it->Free(it);
  InvertedIndex_Free(idx);
}

// Test class for Revalidate functionality of Optional Iterator (Non-optimized)
class OptionalIteratorRevalidateTest : public ::testing::Test {
protected:
  QueryIterator *oi_base;
  MockIterator* mockChild;
  MockQueryEvalCtx* mockCtx;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;
  const double weight = 2.0;

  void SetUp() override {
    // Create child iterator with specific docIds
    mockChild = new MockIterator({10UL, 20UL, 30UL, 40UL, 50UL});
    QueryIterator *child = reinterpret_cast<QueryIterator *>(mockChild);

    // Create non-optimized optional iterator with child
    mockCtx = new MockQueryEvalCtx(maxDocId, numDocs);
    oi_base = NewOptionalIterator(child, &mockCtx->qctx, weight);
  }

  void TearDown() override {
    if (oi_base) {
      oi_base->Free(oi_base);
    }
    delete mockCtx;
  }
};

TEST_F(OptionalIteratorRevalidateTest, RevalidateOK) {
  // Child returns VALIDATE_OK
  mockChild->SetRevalidateResult(VALIDATE_OK);

  // Read a few documents first to establish position
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Revalidate should return VALIDATE_OK
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify child was revalidated
  ASSERT_EQ(mockChild->GetValidationCount(), 1);

  // Should be able to continue reading
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

TEST_F(OptionalIteratorRevalidateTest, RevalidateAborted) {
  // Child returns VALIDATE_ABORTED
  mockChild->SetRevalidateResult(VALIDATE_ABORTED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Optional iterator handles child abort gracefully by replacing with empty iterator
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_OK); // Optional iterator continues even when child is aborted

  // Should be able to continue reading (now all virtual hits)
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

TEST_F(OptionalIteratorRevalidateTest, RevalidateMoved) {
  // Child returns VALIDATE_MOVED
  mockChild->SetRevalidateResult(VALIDATE_MOVED);

  // Read to a real hit (document from child)
  ASSERT_EQ(oi_base->SkipTo(oi_base, 10), ITERATOR_OK);
  ASSERT_EQ(oi_base->lastDocId, 10);

  // Revalidate should handle child movement
  ValidateStatus status = oi_base->Revalidate(oi_base);
  // Should either be OK (if virtual result) or MOVED (if real result was affected)
  ASSERT_TRUE(status == VALIDATE_OK || status == VALIDATE_MOVED);

  // Should be able to continue reading after revalidation
  IteratorStatus read_status = oi_base->Read(oi_base);
  ASSERT_TRUE(read_status == ITERATOR_OK || read_status == ITERATOR_EOF);
}

TEST_F(OptionalIteratorRevalidateTest, RevalidateMovedVirtualResult) {
  // Child returns VALIDATE_MOVED
  mockChild->SetRevalidateResult(VALIDATE_MOVED);

  // Read to a virtual hit (document not in child)
  ASSERT_EQ(oi_base->SkipTo(oi_base, 15), ITERATOR_OK);
  ASSERT_EQ(oi_base->lastDocId, 15);

  // Since current result is virtual, revalidate should return OK
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Should be able to continue reading
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

// Test class for Revalidate functionality of Optimized Optional Iterator
class OptionalIteratorOptimizedRevalidateTest : public ::testing::Test {
protected:
  QueryIterator *oi_base;
  MockIterator* mockChild;
  MockIterator* mockWildcard;
  std::unique_ptr<MockQueryEvalCtx> mockCtx;
  const t_docId maxDocId = 100;
  const double weight = 2.0;

  void SetUp() override {
    // Create child iterator with specific docIds
    std::vector<t_docId> childDocIds = {15, 35, 55, 75}; // Sparse exclusions
    mockChild = new MockIterator(childDocIds);
    QueryIterator *child = reinterpret_cast<QueryIterator *>(mockChild);

    // Create optimized optional iterator (will create wildcard internally)
    std::vector<t_docId> wildcard = {5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95};
    mockCtx = std::make_unique<MockQueryEvalCtx>(wildcard);
    oi_base = NewOptionalIterator(child, &mockCtx->qctx, weight);

    // Replace the wildcard iterator with a mock for testing
    OptionalIterator *oi = (OptionalIterator *)oi_base;
    QueryIterator *wcii = oi->wcii;
    ASSERT_TRUE(wcii != nullptr);
    wcii->Free(wcii); // Free the original wildcard iterator
    mockWildcard = new MockIterator(wildcard);
    oi->wcii = reinterpret_cast<QueryIterator *>(mockWildcard);
  }

  void TearDown() override {
    if (oi_base) {
      oi_base->Free(oi_base);
    }
  }
};

// Test combinations: Child OK, Wildcard OK
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildOK_WildcardOK) {
  mockChild->SetRevalidateResult(VALIDATE_OK);
  mockWildcard->SetRevalidateResult(VALIDATE_OK);

  // Read a few documents first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Revalidate should return VALIDATE_OK
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify both child and wildcard were revalidated
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);

  // Should be able to continue reading
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

// Test combinations: Child OK, Wildcard ABORTED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildOK_WildcardAborted) {
  mockChild->SetRevalidateResult(VALIDATE_OK);
  mockWildcard->SetRevalidateResult(VALIDATE_ABORTED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Revalidate should propagate wildcard abort
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_ABORTED);

  // Verify wildcard was checked (child might not be checked if wildcard aborts first)
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test combinations: Child OK, Wildcard MOVED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildOK_WildcardMoved) {
  mockChild->SetRevalidateResult(VALIDATE_OK);
  mockWildcard->SetRevalidateResult(VALIDATE_MOVED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Revalidate should handle wildcard movement
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_MOVED);

  // Verify both iterators were checked
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test combinations: Child ABORTED, Wildcard OK
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildAborted_WildcardOK) {
  mockChild->SetRevalidateResult(VALIDATE_ABORTED);
  mockWildcard->SetRevalidateResult(VALIDATE_OK);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Optimized optional iterator handles child abort gracefully
  ValidateStatus status = oi_base->Revalidate(oi_base);
  //////// Cannot access `mockChild` after it has been replaced
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify both iterators were checked
  ASSERT_EQ(reinterpret_cast<OptionalIterator *>(oi_base)->child->type, EMPTY_ITERATOR);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);

  // Should be able to continue reading (all wildcard docs now virtual)
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

// Test combinations: Child ABORTED, Wildcard ABORTED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildAborted_WildcardAborted) {
  mockChild->SetRevalidateResult(VALIDATE_ABORTED);
  mockWildcard->SetRevalidateResult(VALIDATE_ABORTED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Both iterators aborted - optimized optional should abort due to wildcard
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_ABORTED);

  // Verify wildcard was checked (child might not be checked if wildcard aborts first)
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test combinations: Child ABORTED, Wildcard MOVED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildAborted_WildcardMoved) {
  mockChild->SetRevalidateResult(VALIDATE_ABORTED);
  mockWildcard->SetRevalidateResult(VALIDATE_MOVED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Child aborted but wildcard moved - should handle gracefully
  ValidateStatus status = oi_base->Revalidate(oi_base);
  //////// Cannot access `mockChild` after it has been replaced
  ASSERT_EQ(status, VALIDATE_MOVED);

  // Verify both iterators were checked
  ASSERT_EQ(reinterpret_cast<OptionalIterator *>(oi_base)->child->type, EMPTY_ITERATOR);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test combinations: Child MOVED, Wildcard OK
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildMoved_WildcardOK) {
  mockChild->SetRevalidateResult(VALIDATE_MOVED);
  mockWildcard->SetRevalidateResult(VALIDATE_OK);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Child moved but wildcard OK
  ValidateStatus status = oi_base->Revalidate(oi_base);
  // Should return OK since wildcard didn't move and determines position
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify both iterators were checked
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);

  // Should be able to continue reading
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

// Test combinations: Child MOVED, Wildcard ABORTED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildMoved_WildcardAborted) {
  mockChild->SetRevalidateResult(VALIDATE_MOVED);
  mockWildcard->SetRevalidateResult(VALIDATE_ABORTED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Child moved but wildcard aborted - optimized optional needs wildcard
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_ABORTED);

  // Verify wildcard was checked (child might not be checked if wildcard aborts first)
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test combinations: Child MOVED, Wildcard MOVED
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildMoved_WildcardMoved) {
  mockChild->SetRevalidateResult(VALIDATE_MOVED);
  mockWildcard->SetRevalidateResult(VALIDATE_MOVED);

  // Read a document first
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);

  // Both iterators moved
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_MOVED);

  // Verify both iterators were checked
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);

  // Should be able to continue reading
  ASSERT_EQ(oi_base->Read(oi_base), ITERATOR_OK);
}

// Test specific scenario: wildcard not moved, child moved, current result is real
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateChildMovedRealResult_WildcardOK) {
  mockChild->SetRevalidateResult(VALIDATE_MOVED);
  mockWildcard->SetRevalidateResult(VALIDATE_OK);

  // Position at a real hit from child (15 is in both child and wildcard)
  ASSERT_EQ(oi_base->SkipTo(oi_base, 15), ITERATOR_OK);
  ASSERT_EQ(oi_base->lastDocId, 15);

  // Verify we have a real result from child before revalidation
  OptionalIterator *oi = (OptionalIterator *)oi_base;
  ASSERT_EQ(oi_base->current, oi->child->current); // Real result from child

  // Revalidate: child moved but wildcard OK
  // Since current result was real and child moved, should return OK
  // (wildcard determines position in optimized mode)
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_EQ(oi_base->lastDocId, 20); // Should move to next valid position
  ASSERT_EQ(oi_base->current, oi->virt); // Should now be virtual result

  // Verify both iterators were checked
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);
}

// Test specific scenario: wildcard moved ahead to ID that child also has
TEST_F(OptionalIteratorOptimizedRevalidateTest, RevalidateWildcardMovedToChildId) {
  // Configure child and wildcard movements
  mockChild->SetRevalidateResult(VALIDATE_OK);
  mockWildcard->SetRevalidateResult(VALIDATE_MOVED);

  // Start at position 5 (first wildcard doc, not in child)
  ASSERT_EQ(oi_base->SkipTo(oi_base, 10), ITERATOR_OK);
  ASSERT_EQ(oi_base->lastDocId, 10);

  // Verify we have a virtual result before revalidation
  OptionalIterator *oi = (OptionalIterator *)oi_base;
  ASSERT_EQ(oi_base->current, oi->virt); // Virtual result

  // When revalidate is called:
  // - mockWildcard will move from 5 to next in sequence (10)
  // - mockChild will move from 15 (first in its sequence) to next (35)
  // The optional iterator should handle both movements
  ValidateStatus status = oi_base->Revalidate(oi_base);
  ASSERT_EQ(status, VALIDATE_MOVED);

  // Verify both iterators were checked
  ASSERT_EQ(mockChild->GetValidationCount(), 1);
  ASSERT_EQ(mockWildcard->GetValidationCount(), 1);

  // After revalidation, iterator position should be updated
  // The exact position depends on implementation but should be valid
  ASSERT_EQ(oi_base->lastDocId, 15); // Should have moved forward
  ASSERT_EQ(oi_base->current, oi->child->current); // Should now be a real result from child
}
