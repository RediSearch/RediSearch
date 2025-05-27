/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#include "rmutil/alloc.h"

#include "gtest/gtest.h"

#include "src/iterators/idlist_iterator.h"

static bool cmp_docids(const t_docId& d1, const t_docId& d2) {
  return d1 < d2;
}

class IDListIteratorCommonTest : public ::testing::TestWithParam<std::vector<t_docId>> {
protected:
  std::vector<t_docId> docIds;
  QueryIterator *iterator_base;

  void SetUp() override {
    docIds = GetParam();
      iterator_base = IT_V2(NewIdListIterator)(docIds.data(), docIds.size(), 1.0);
  }
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};


TEST_P(IDListIteratorCommonTest, Read) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  IdListIterator *iterator = (IdListIterator *)iterator_base;
  IteratorStatus rc;
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), docIds.size());

  // Test reading until EOF
  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
      ASSERT_EQ(iterator->base.current->docId, sorted_docIds[i]);
      ASSERT_EQ(iterator->base.lastDocId, sorted_docIds[i]);
      ASSERT_FALSE(iterator->base.atEOF);
      i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator->base.atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sorted_docIds[0]), ITERATOR_EOF); // SkipTo after EOF should return EOF
  ASSERT_EQ(i, docIds.size()) << "Expected to read " << docIds.size() << " documents";
}

TEST_P(IDListIteratorCommonTest, SkipTo) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  IdListIterator *iterator = (IdListIterator *)iterator_base;
  IteratorStatus rc;

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds[0]);
  ASSERT_EQ(iterator->offset, 1);
  ASSERT_FALSE(iterator->base.atEOF);

  // Skip To to higher than last docID returns EOF, but the lastDocId and EOF is not updated
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sorted_docIds.back() + 1), ITERATOR_EOF);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds[0]);
  ASSERT_EQ(iterator->offset, 1);
  ASSERT_TRUE(iterator->base.atEOF);

  iterator_base->Rewind(iterator_base);

  t_docId i = 1;
  for (size_t index = 0; index < sorted_docIds.size(); index++) {
    t_docId id = sorted_docIds[index];
    while (i < id) {
      // Skip To from last sorted_id to the next one, should move the current iterator to id
      iterator_base->Rewind(iterator_base);
      rc = iterator_base->SkipTo(iterator_base, i);
      ASSERT_EQ(rc, ITERATOR_NOTFOUND);
      ASSERT_EQ(iterator->base.lastDocId, id);
      ASSERT_EQ(iterator->base.current->docId, id);
      ASSERT_EQ(iterator->offset, index + 1); //offset is pointing to the next already
      ASSERT_EQ(iterator->base.atEOF, index == sorted_docIds.size() - 1); // When I skip towards the last one, it sets to the EOF
      iterator_base->Rewind(iterator_base);
      i++;
    }
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
    ASSERT_EQ(iterator->offset, index + 1); //offset is pointing to the next already
    ASSERT_EQ(iterator->base.atEOF, index == sorted_docIds.size() - 1); // When I skip towards the last one, it sets to the EOF
    i++;
  }

  iterator_base->Rewind(iterator_base);
  for (t_docId id : sorted_docIds) {
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
  }
}

TEST_P(IDListIteratorCommonTest, Rewind) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  IdListIterator *iterator = (IdListIterator *)iterator_base;
  IteratorStatus rc;
  for (t_docId id : sorted_docIds) {
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator->base.lastDocId, 0);
    ASSERT_FALSE(iterator->base.atEOF);
  }
  for (t_docId id : sorted_docIds) {
    rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
  }
  // Rewind after EOF read
  rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator->base.atEOF);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds.back());
  ASSERT_EQ(iterator->base.current->docId, sorted_docIds.back());
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator->base.lastDocId, 0);
  ASSERT_FALSE(iterator->base.atEOF);
}

// Parameters for the tests above. Some set of docIDs sorted and unsorted
INSTANTIATE_TEST_SUITE_P(IDListIteratorP, IDListIteratorCommonTest, ::testing::Values(
        std::vector<t_docId>{1, 2, 3, 40, 50},
        std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2}
    )
);
