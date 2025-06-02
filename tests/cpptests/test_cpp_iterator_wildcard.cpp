/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#include <algorithm>
#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/iterators/wildcard_iterator.h"

class WildcardIteratorTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;
  const t_docId maxDocId = 100;
  const size_t numDocs = 50;

  void SetUp() override {
    iterator_base = IT_V2(NewWildcardIterator_NonOptimized)(maxDocId, numDocs);
  }
  
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_F(WildcardIteratorTest, InitialState) {
  WildcardIterator *wi = (WildcardIterator *)iterator_base;
  
  // Test initial state
  ASSERT_EQ(wi->topId, maxDocId);
  ASSERT_EQ(wi->numDocs, numDocs);
  ASSERT_EQ(wi->currentId, 0);
  ASSERT_FALSE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_EQ(iterator_base->type, WILDCARD_ITERATOR);
  
  // Test NumEstimated returns the correct number of docs
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), numDocs);
}

TEST_F(WildcardIteratorTest, Read) {
  // Test sequential reads
  for (t_docId i = 1; i <= maxDocId; i++) {
    ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, i);
  }
  
  // After reading all docs, should return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  
  // Reading again should still return EOF
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
}

TEST_F(WildcardIteratorTest, SkipTo) {
  // Test skipping to specific docIds
  t_docId skipTargets[] = {5, 10, 20, 50, 75, 100};
  
  for (size_t i = 0; i < sizeof(skipTargets) / sizeof(skipTargets[0]); i++) {
    t_docId target = skipTargets[i];
    ASSERT_EQ(iterator_base->SkipTo(iterator_base, target), ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, target);
  }
  
  // Skip beyond maxDocId should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, maxDocId + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(WildcardIteratorTest, SkipToZero) {
  // Test skipping to docId 0 (should read next doc, which is 1)
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 0), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
}

TEST_F(WildcardIteratorTest, Rewind) {
  // First read some docs
  for (int i = 0; i < 10; i++) {
    iterator_base->Read(iterator_base);
  }
  ASSERT_EQ(iterator_base->current->docId, 10);
  ASSERT_EQ(((WildcardIterator *)iterator_base)->currentId, 10);
  // Test that Rewind resets the iterator
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(((WildcardIterator *)iterator_base)->currentId, 0);
  ASSERT_FALSE(iterator_base->atEOF);
  
  // After Rewind, should be able to read from the beginning
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 1);
}

TEST_F(WildcardIteratorTest, ReadAfterSkip) {
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

TEST_F(WildcardIteratorTest, SkipBackwards) {
  // Skip forward
  iterator_base->SkipTo(iterator_base, 50);
  ASSERT_EQ(iterator_base->current->docId, 50);
  
  // Skip backwards (should maintain position)
  iterator_base->SkipTo(iterator_base, 25);
  ASSERT_EQ(iterator_base->current->docId, 25);
  
  // Continue reading from new position
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, 26);
}

TEST_F(WildcardIteratorTest, ResultProperties) {
  // Test that the result has the expected properties
  iterator_base->Read(iterator_base);
  ASSERT_EQ(iterator_base->current->docId, 1);
  ASSERT_EQ(iterator_base->current->freq, 1);
  ASSERT_EQ(iterator_base->current->fieldMask, RS_FIELDMASK_ALL);
}

TEST_F(WildcardIteratorTest, ZeroDocuments) {
  // Create a wildcard iterator with zero documents
  QueryIterator *emptyIterator = IT_V2(NewWildcardIterator_NonOptimized)(0, 0);
  
  // Should immediately return EOF on read
  ASSERT_EQ(emptyIterator->Read(emptyIterator), ITERATOR_EOF);
  ASSERT_TRUE(emptyIterator->atEOF);
  
  // Should return EOF on skip
  ASSERT_EQ(emptyIterator->SkipTo(emptyIterator, 1), ITERATOR_EOF);
  
  emptyIterator->Free(emptyIterator);
}
