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
  t_docId not_present_docId = 99999;
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  IdListIterator *iterator = (IdListIterator *)iterator_base;
  IteratorStatus rc;
  //TODO(Joan): Check if EOF should be updated when ITERATOR_OK is not returned

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds[0]);
  ASSERT_EQ(iterator->offset, 1);
  ASSERT_FALSE(iterator->base.atEOF);
  // Skip To to higher than last docID returns EOF, but the lastDocId and EOF is not updated
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sorted_docIds.back() + 1), ITERATOR_EOF);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds[0]);
  ASSERT_EQ(iterator->offset, 1);
  ASSERT_FALSE(iterator->base.atEOF);

  //Skip To to a docID that does not exist returns NOTFOUND, but the lastDocId and EOF? is not updated
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, not_present_docId), ITERATOR_NOTFOUND);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds.back());
  ASSERT_EQ(iterator->offset, sorted_docIds.size());
  ASSERT_FALSE(iterator->base.atEOF);
  
  //TODO(Joan): Add more tests here
}

TEST_P(IDListIteratorCommonTest, Rewind) {
  //TODO(Joan): Implement here

}

// Parameters for the tests above. Some set of docIDs sorted and unsorted
INSTANTIATE_TEST_SUITE_P(IDListIteratorP, IDListIteratorCommonTest, ::testing::Values(
        std::vector<t_docId>{1, 2, 3, 40, 50},
        std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2}
    )
);
