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
#include "iterator_util.h"

#include "src/iterators/optional_iterator.h"
#include "src/iterators/empty_iterator.h"
#include "src/iterators/idlist_iterator.h"

// Test optional iterator with no child (empty child)
class OptionalIteratorEmptyTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;
  const double weight = 1.5;

  void SetUp() override {
    // Create optional iterator with null child (will use empty iterator internally)
    iterator_base = IT_V2(NewOptionalIterator_NonOptimized)(NULL, maxDocId, numDocs, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
  }
};

TEST_F(OptionalIteratorEmptyTest, BasicCreation) {
  // Test that the iterator was created successfully
  ASSERT_NE(iterator_base, nullptr);
  ASSERT_EQ(iterator_base->type, OPTIONAL_ITERATOR);
  ASSERT_FALSE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->lastDocId, 0);

  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(oi->maxDocId, maxDocId);
  ASSERT_EQ(oi->weight, weight);
  ASSERT_EQ(oi->child->type, EMPTY_ITERATOR);  // Should have empty iterator as child
  ASSERT_NE(oi->virt, nullptr);   // Should have virtual result

  // Test NumEstimated returns maxDocId
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), maxDocId);
}

TEST_F(OptionalIteratorEmptyTest, ReadSequential) {
  // Test sequential reads - should return virtual results for all docIds

  for (t_docId i = 1; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_NE(iterator_base->current, nullptr);
    ASSERT_EQ(iterator_base->current->docId, i);
    ASSERT_EQ(iterator_base->lastDocId, i);
    // Should be virtual result since child is empty
    OptionalIterator *oi = (OptionalIterator *)iterator_base;
    ASSERT_EQ(iterator_base->current, oi->virt);
    ASSERT_EQ(iterator_base->current->freq, 1);
    ASSERT_EQ(iterator_base->current->fieldMask, RS_FIELDMASK_ALL);
  }

  // After reading all docs, should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(OptionalIteratorEmptyTest, SkipTo) {
  // Test skipping to specific docIds
  std::vector<t_docId> skipTargets = {5, 10, 20, 50, 75, 100};

  for (size_t i = 0; i < skipTargets.size(); i++) {
    t_docId target = skipTargets[i];
    ASSERT_EQ(iterator_base->SkipTo(iterator_base, target), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, target);
    ASSERT_EQ(iterator_base->lastDocId, target);

    // Should be virtual result
    OptionalIterator *oi = (OptionalIterator *)iterator_base;
    ASSERT_EQ(iterator_base->current, oi->virt);
  }

  // Skip beyond maxDocId should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(OptionalIteratorEmptyTest, SkipToZero) {
  // Test skipping to docId 0 (should set to 0)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 0), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 0);
  ASSERT_EQ(iterator_base->lastDocId, 0);
}

TEST_F(OptionalIteratorEmptyTest, Rewind) {
  // First read some docs
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

TEST_F(OptionalIteratorEmptyTest, ReadAfterSkip) {
  // Skip to middle
  iterator_base->SkipTo(iterator_base, 50);
  ASSERT_EQ(iterator_base->current->docId, 50);

  // Continue reading sequentially
  for (t_docId i = 51; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
  }

  // After reading all docs, should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
}

// Test optional iterator with a real child iterator
class OptionalIteratorWithChildTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  std::vector<t_docId> childDocIds;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;
  const double weight = 2.0;

  void SetUp() override {
    // Create child iterator with specific docIds
    childDocIds = {10, 20, 30, 50, 80};
    QueryIterator *child = IT_V2(NewIdListIterator)(childDocIds.data(), childDocIds.size(), 1.0);

    // Create optional iterator with child
    iterator_base = IT_V2(NewOptionalIterator_NonOptimized)(child, maxDocId, numDocs, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
  }
};

TEST_F(OptionalIteratorWithChildTest, ReadMixedResults) {
  // Test reading - should return mix of real and virtual results

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

TEST_F(OptionalIteratorWithChildTest, SkipToRealHit) {
  // Skip to a docId that exists in child
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 20), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 20);
  ASSERT_EQ(iterator_base->current->weight, weight);
  ASSERT_EQ(iterator_base->lastDocId, 20);

  // Should be real hit from child
  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(iterator_base->current, oi->child->current);
}

TEST_F(OptionalIteratorWithChildTest, SkipToVirtualHit) {
  // Skip to a docId that doesn't exist in child
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 25), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 25);
  ASSERT_EQ(iterator_base->lastDocId, 25);

  // Should be virtual hit
  OptionalIterator *oi = (OptionalIterator *)iterator_base;
  ASSERT_EQ(iterator_base->current, oi->virt);
}

TEST_F(OptionalIteratorWithChildTest, SkipToSequence) {
  // Test skipping to various docIds in sequence
  t_docId targets[] = {5, 15, 25, 35, 45, 55, 65, 75, 85, 95};

  for (size_t i = 0; i < sizeof(targets) / sizeof(targets[0]); i++) {
    t_docId target = targets[i];
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

TEST_F(OptionalIteratorWithChildTest, RewindBehavior) {
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

TEST_F(OptionalIteratorWithChildTest, EOFBehavior) {
  // Test EOF when reaching maxDocId
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, maxDocId);
  ASSERT_EQ(iterator_base->lastDocId, maxDocId);

  // Next read should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  // Further operations should still return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 1), ITERATOR_EOF);
}

TEST_F(OptionalIteratorWithChildTest, WeightApplication) {
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

TEST_F(OptionalIteratorWithChildTest, VirtualResultWeight) {
  // Test that virtual results have the correct weight
  OptionalIterator *oi = (OptionalIterator *)iterator_base;

  // Skip to a virtual hit (not in childDocIds)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 15), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current, oi->virt);
  ASSERT_EQ(iterator_base->current->weight, weight);
}

TEST_F(OptionalIteratorEmptyTest, VirtualResultWeightEmpty) {
  // Test that virtual results have the correct weight when no child
  OptionalIterator *oi = (OptionalIterator *)iterator_base;

  // Read first document (should be virtual)
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current, oi->virt);
  ASSERT_EQ(iterator_base->current->weight, weight);
}

// Test edge cases
class OptionalIteratorEdgeCasesTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  const t_docId maxDocId = 5;  // Small range for edge case testing
  const size_t numDocs = 3;
  const double weight = 3.0;

  void SetUp() override {
    // Create optional iterator with small range for edge case testing
    iterator_base = IT_V2(NewOptionalIterator_NonOptimized)(NULL, maxDocId, numDocs, weight);
  }

  void TearDown() override {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
    }
  }
};

TEST_F(OptionalIteratorEdgeCasesTest, SkipBeyondMax) {
  // Test skipping beyond maxDocId
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  // Further operations should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 1), ITERATOR_EOF);
}

TEST_F(OptionalIteratorEdgeCasesTest, ReadToEnd) {
  // Read all documents to EOF
  for (t_docId i = 1; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
  }

  // Next read should be EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(OptionalIteratorEdgeCasesTest, SkipToCurrentPosition) {
  // Read to position 3
  for (int i = 0; i < 3; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->lastDocId, 3);

  // Skip to same position
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 3), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 3);
  ASSERT_EQ(iterator_base->lastDocId, 3);
}

TEST_F(OptionalIteratorEdgeCasesTest, SkipBackwards) {
  // Read to position 4
  for (int i = 0; i < 4; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->lastDocId, 4);

  // Skip to earlier position (should still work)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 2), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 2);
  ASSERT_EQ(iterator_base->lastDocId, 2);
}
