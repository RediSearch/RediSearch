/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include <algorithm>
#include "gtest/gtest.h"
#include "iterators_rs.h"

static bool cmp_docids(const t_docId& d1, const t_docId& d2) {
  return d1 < d2;
}

class IDListIteratorCommonTest : public ::testing::TestWithParam<std::vector<t_docId>> {
protected:
  std::vector<t_docId> docIds;
  QueryIterator *iterator_base;

  void SetUp() override {
    docIds = GetParam();
    // do a copy of the docIds vector and sort them before passing them to the iterator
    std::sort(docIds.begin(), docIds.end(), cmp_docids);
    // remove duplicates
    docIds.erase(std::unique(docIds.begin(), docIds.end()), docIds.end());
    //do a copy of the docIds vector before passing it to iterator
    t_docId* ids_array = (t_docId*)rm_malloc(docIds.size() * sizeof(t_docId));
    std::copy(docIds.begin(), docIds.end(), ids_array);
    iterator_base = NewSortedIdListIterator(ids_array, docIds.size(), 1.0);
  }
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_P(IDListIteratorCommonTest, Revalidate) {
  ASSERT_EQ(iterator_base->Revalidate(iterator_base), VALIDATE_OK);
}


TEST_P(IDListIteratorCommonTest, Read) {
  IteratorStatus rc;
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), docIds.size());

  // Test reading until EOF
  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
    ASSERT_EQ(iterator_base->current->docId, docIds[i]);
    ASSERT_EQ(iterator_base->lastDocId, docIds[i]);
    ASSERT_FALSE(iterator_base->atEOF);
    i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, docIds[0]), ITERATOR_EOF); // SkipTo after EOF should return EOF
  ASSERT_EQ(i, docIds.size()) << "Expected to read " << docIds.size() << " documents";
}

TEST_P(IDListIteratorCommonTest, SkipTo) {
  IteratorStatus rc;

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, docIds[0]);
  ASSERT_EQ(iterator_base->lastDocId, docIds[0]);
  ASSERT_FALSE(iterator_base->atEOF);

  // Skip To to higher than last docID returns EOF, but the lastDocId and EOF is not updated
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, docIds.back() + 1), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->current->docId, docIds[0]);
  ASSERT_EQ(iterator_base->lastDocId, docIds[0]);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);

  t_docId i = 1;
  for (size_t index = 0; index < docIds.size(); index++) {
    t_docId id = docIds[index];
    while (i < id) {
      // Skip To from last sorted_id to the next one, should move the current iterator to id
      iterator_base->Rewind(iterator_base);
      rc = iterator_base->SkipTo(iterator_base, i);
      ASSERT_EQ(rc, ITERATOR_NOTFOUND);
      ASSERT_EQ(iterator_base->current->docId, id);
      ASSERT_EQ(iterator_base->lastDocId, id);
      ASSERT_FALSE(iterator_base->atEOF); // EOF would be set in another iteration
      iterator_base->Rewind(iterator_base);
      i++;
    }
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
    ASSERT_FALSE(iterator_base->atEOF); // EOF would be set in another iteration
    i++;
  }
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);
  for (t_docId id : docIds) {
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
  }
}

TEST_P(IDListIteratorCommonTest, Rewind) {
  IteratorStatus rc;
  for (t_docId id : docIds) {
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->lastDocId, 0);
    ASSERT_FALSE(iterator_base->atEOF);
  }
  for (t_docId id : docIds) {
    rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
  }
  // Rewind after EOF read
  rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->current->docId, docIds.back());
  ASSERT_EQ(iterator_base->lastDocId, docIds.back());
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);
}

TEST_P(IDListIteratorCommonTest, SkipBetweenAnyPair) {
  // Test skipping between arbitrary pairs (forward only)
  for (size_t fromIdx = 0; fromIdx < docIds.size() - 1; fromIdx++) {
    for (size_t toIdx = fromIdx + 1; toIdx < docIds.size(); toIdx++) {
      iterator_base->Rewind(iterator_base);

      t_docId fromId = docIds[fromIdx];
      t_docId toId = docIds[toIdx];

      // First skip to fromId
      ASSERT_EQ(iterator_base->SkipTo(iterator_base, fromId), ITERATOR_OK);
      ASSERT_EQ(iterator_base->current->docId, fromId);
      ASSERT_EQ(iterator_base->lastDocId, fromId);

      // Then skip to toId
      ASSERT_EQ(iterator_base->SkipTo(iterator_base, toId), ITERATOR_OK);
      ASSERT_EQ(iterator_base->current->docId, toId);
      ASSERT_EQ(iterator_base->lastDocId, toId);
    }
  }
}

// Parameters for the tests above. Some set of docIDs sorted and unsorted
INSTANTIATE_TEST_SUITE_P(IDListIteratorP, IDListIteratorCommonTest, ::testing::Values(
        std::vector<t_docId>{1, 2, 3, 40, 50},
        std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2},
        std::vector<t_docId>{42},
        std::vector<t_docId>{1000000, 2000000, 3000000},
        std::vector<t_docId>{10, 20, 30, 40, 50},
        std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2},
        std::vector<t_docId>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40}
    )
);
