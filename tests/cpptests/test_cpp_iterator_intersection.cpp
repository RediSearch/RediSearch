/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "gtest/gtest.h"
#include <map>

#include "rmutil/alloc.h"
#include "iterator_util.h"

#include "src/iterators/intersection_iterator.h"
#include "src/iterators/inverted_index_iterator.h"
#include "inverted_index.h"
#include "src/iterators/empty_iterator.h"
#include "src/iterators/wildcard_iterator.h"
#include "src/forward_index.h"

class IntersectionIteratorCommonTest : public ::testing::TestWithParam<std::tuple<unsigned, std::vector<t_docId>>> {
protected:
  std::vector<std::vector<t_docId>> docIds;
  std::vector<t_docId> resultSet;
  QueryIterator *ii_base;

  void SetUp() override {
    unsigned numChildren;
    std::tie(numChildren, resultSet) = GetParam();
    // Verify the resultSet is sorted and unique
    std::sort(resultSet.begin(), resultSet.end());
    resultSet.erase(std::unique(resultSet.begin(), resultSet.end()), resultSet.end());
    // Set docIds so the intersection of all children is resultSet.
    // Make sure that some ids are unique to each child
    docIds.resize(numChildren);
    t_docId id = 1;
    for (auto &childIds : docIds) {
      // Copy the resultSet to each child as a base
      childIds = resultSet;
      // Add some unique ids to each child. Mock constructor will ensure that the ids are unique and sorted.
      for (size_t i = 0; i < 100; i++) {
        childIds.push_back(id++);
      }
    }
    // Create children iterators
    auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * numChildren);
    for (unsigned i = 0; i < numChildren; i++) {
      auto cur = new MockIterator(docIds[i]);
      docIds[i] = cur->docIds; // Ensure that the docIds are unique and sorted
      children[i] = reinterpret_cast<QueryIterator *>(cur);
    }
    // Create an intersection iterator
    ii_base = NewIntersectionIterator(children, numChildren, -1, false, 1.0);
  }
  void TearDown() override {
    ii_base->Free(ii_base);
  }
};

TEST_P(IntersectionIteratorCommonTest, Read) {
  IntersectionIterator *ii = (IntersectionIterator *)ii_base;
  IteratorStatus rc;

  // Verify that the child iterators are sorted correctly by the estimated number of results
  for (uint32_t i = 1; i < ii->num_its; i++) {
    auto prev_est = ii->its[i - 1]->NumEstimated(ii->its[i - 1]);
    auto cur_est = ii->its[i]->NumEstimated(ii->its[i]);
    EXPECT_LE(prev_est, cur_est) << "Child iterators are not sorted by estimated results";
  }

  // Test reading until EOF
  size_t i = 0;
  while ((rc = ii_base->Read(ii_base)) == ITERATOR_OK) {
    ASSERT_EQ(ii->base.current->docId, resultSet[i]);
    ASSERT_EQ(ii->base.lastDocId, resultSet[i]);
    ASSERT_FALSE(ii->base.atEOF);
    i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ii->base.atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(i, resultSet.size()) << "Expected to read " << resultSet.size() << " documents";

  size_t expected = SIZE_MAX;
  for (auto &child : docIds) {
    expected = std::min(expected, child.size());
  }
  ASSERT_EQ(ii_base->NumEstimated(ii_base), expected);
}

TEST_P(IntersectionIteratorCommonTest, SkipTo) {
  IntersectionIterator *ii = (IntersectionIterator *)ii_base;
  IteratorStatus rc;
  // Test skipping to any id between 1 and the last id
  t_docId i = 1;
  for (t_docId id : resultSet) {
    while (i < id) {
      ii_base->Rewind(ii_base);
      rc = ii_base->SkipTo(ii_base, i);
      ASSERT_EQ(rc, ITERATOR_NOTFOUND);
      ASSERT_EQ(ii->base.lastDocId, id);
      ASSERT_EQ(ii->base.current->docId, id);
      i++;
    }
    ii_base->Rewind(ii_base);
    rc = ii_base->SkipTo(ii_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(ii->base.lastDocId, id);
    ASSERT_EQ(ii->base.current->docId, id);
    i++;
  }
  // Test reading after skipping to the last id
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, ii_base->lastDocId + 1), ITERATOR_EOF);
  ASSERT_TRUE(ii->base.atEOF);

  ii_base->Rewind(ii_base);
  ASSERT_EQ(ii->base.lastDocId, 0);
  ASSERT_FALSE(ii->base.atEOF);
  // Test skipping to all ids that exist
  for (t_docId id : resultSet) {
    rc = ii_base->SkipTo(ii_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(ii->base.lastDocId, id);
    ASSERT_EQ(ii->base.current->docId, id);
  }

  // Test skipping to an id that exceeds the last id
  ii_base->Rewind(ii_base);
  ASSERT_EQ(ii->base.lastDocId, 0);
  ASSERT_FALSE(ii->base.atEOF);
  rc = ii_base->SkipTo(ii_base, resultSet.back() + 1);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_EQ(ii->base.lastDocId, 0); // we just rewound
  ASSERT_TRUE(ii->base.atEOF);
}

TEST_P(IntersectionIteratorCommonTest, Rewind) {
  IntersectionIterator *ii = (IntersectionIterator *)ii_base;
  IteratorStatus rc;
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j <= i; j++) {
      ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
      ASSERT_EQ(ii->base.current->docId, resultSet[j]);
      ASSERT_EQ(ii->base.lastDocId, resultSet[j]);
    }
    ii_base->Rewind(ii_base);
    ASSERT_EQ(ii->base.lastDocId, 0);
    ASSERT_FALSE(ii->base.atEOF);
  }
}

// Parameters for the tests above. We run all the combinations of:
// 1. number of child iterators in {2, 5, 25}
// 2. expected result set, one of the 3 given lists below
INSTANTIATE_TEST_SUITE_P(IntersectionIterator, IntersectionIteratorCommonTest, ::testing::Combine(
  ::testing::Values(2, 5, 25),
  ::testing::Values(
    std::vector<t_docId>{1, 2, 3, 40, 50},
    std::vector<t_docId>{5, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012, 12345, 23456, 34567, 45678, 56789},
    std::vector<t_docId>{9, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250}
  )
));

class IntersectionIteratorTest : public ::testing::Test {
protected:
  QueryIterator *ii_base;
  std::map<std::string, InvertedIndex *> invertedIndexes;
  t_docId num_docs;

  void SetUp() override {
    num_docs = 0;
    ii_base = nullptr;
  }
  void TearDown() override {
    if (ii_base != nullptr) {
      ii_base->Free(ii_base);
    }
    for (auto &[_, index] : invertedIndexes) {
      InvertedIndex_Free(index);
    }
  }

public:
  void CreateIntersectionIterator(const std::vector<std::string> &terms, int max_slop = -1, bool in_order = false) {
    QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * terms.size());
    for (size_t i = 0; i < terms.size(); i++) {
      ASSERT_NE(invertedIndexes.find(terms[i]), invertedIndexes.end()) << "Term " << terms[i] << " not found in inverted indexes";
      children[i] = NewInvIndIterator_TermQuery(invertedIndexes[terms[i]], NULL, {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL}, NULL, 1.0);
    }
    ii_base = NewIntersectionIterator(children, terms.size(), max_slop, in_order, 1.0);
  }
  void AddDocument(const std::vector<std::string> &terms) {
    size_t dummy;
    for (auto &term : terms) {
      if (invertedIndexes.find(term) == invertedIndexes.end()) {
        // Create a new inverted index for the term if it doesn't exist
        invertedIndexes[term] = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS | Index_WideSchema), &dummy);
      }
    }
    t_docId docId = ++num_docs;
    std::map<std::string, ForwardIndexEntry> entries;
    // Add a document to all inverted indexes
    for (size_t i = 0; i < terms.size(); i++) {
      // Get (create if not exists) the forward index entry for the term
      ForwardIndexEntry &entry = entries[terms[i]];
      entry.docId = docId;
      entry.freq++;
      entry.fieldMask = RS_FIELDMASK_ALL;
      if (entry.vw == NULL) {
          entry.vw = NewVarintVectorWriter(8);
      }
      VVW_Write(entry.vw, i + 1); // Store the term index
    }
    // Write the forward index entries to the inverted indexes
    for (auto &[term, entry] : entries) {
      InvertedIndex *index = invertedIndexes[term];
      InvertedIndex_WriteForwardIndexEntry(index, &entry);
      // Free the entry's vector writer
      VVW_Free(entry.vw);
    }
  }
};

TEST_F(IntersectionIteratorTest, NullChildren) {
  QueryIterator **children = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
  children[0] = nullptr;
  children[1] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  ii_base = NewIntersectionIterator(children, 2, -1, false, 1.0);
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->NumEstimated(ii_base), 0);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ii_base->Free(ii_base);

  children = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
  children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[1] = nullptr;
  ii_base = NewIntersectionIterator(children, 2, -1, false, 1.0);
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->NumEstimated(ii_base), 0);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  // No explicit Free call here, the iterator is freed in the TearDown method
}

TEST_F(IntersectionIteratorTest, Slop) {
  // Add documents
  AddDocument({"foo", "bar"});
  AddDocument({"foo", "baz"});
  AddDocument({"bar", "foo"});
  AddDocument({"foo", "baz", "bar"});

  // Create an intersection iterator with slop
  CreateIntersectionIterator({"foo", "bar"}, 0, false);
  ASSERT_EQ(ii_base->type, INTERSECT_ITERATOR);
  ASSERT_EQ(ii_base->NumEstimated(ii_base), 3); // 3 documents match "bar"

  // Read the results. Expected: 1, 3 (slop 0, no order)
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 3);
  ASSERT_EQ(ii_base->lastDocId, 3);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF); // Reading after EOF should return EOF again

  // Rewind and check again with SkipTo
  ii_base->Rewind(ii_base);
  ASSERT_EQ(ii_base->lastDocId, 0);
  ASSERT_FALSE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 2), ITERATOR_NOTFOUND);
  ASSERT_EQ(ii_base->current->docId, 3);
  ASSERT_EQ(ii_base->lastDocId, 3);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 4), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 5), ITERATOR_EOF); // Skipping beyond the last docId should return EOF
  ASSERT_EQ(ii_base->lastDocId, 3); // Last docId should remain unchanged
  ASSERT_TRUE(ii_base->atEOF); // atEOF should remain true
}

TEST_F(IntersectionIteratorTest, InOrder) {
  // Add documents
  AddDocument({"foo", "bar"});
  AddDocument({"foo", "baz"});
  AddDocument({"bar", "foo"});
  AddDocument({"foo", "baz", "bar"});

  // Create an intersection iterator with in-order
  CreateIntersectionIterator({"foo", "bar"}, -1, true);
  ASSERT_EQ(ii_base->type, INTERSECT_ITERATOR);
  ASSERT_EQ(ii_base->NumEstimated(ii_base), 3); // 3 documents match "bar"

  // Read the results. Expected: 1, 4 (any slop, in order)
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 4);
  ASSERT_EQ(ii_base->lastDocId, 4);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF); // Reading after EOF should return EOF again

  // Rewind and check again with SkipTo
  ii_base->Rewind(ii_base);
  ASSERT_EQ(ii_base->lastDocId, 0);
  ASSERT_FALSE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 2), ITERATOR_NOTFOUND);
  ASSERT_EQ(ii_base->current->docId, 4);
  ASSERT_EQ(ii_base->lastDocId, 4);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 5), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 6), ITERATOR_EOF); // Skipping beyond the last docId should return EOF
  ASSERT_EQ(ii_base->lastDocId, 4); // Last docId should remain unchanged
  ASSERT_TRUE(ii_base->atEOF); // atEOF should remain true
}

TEST_F(IntersectionIteratorTest, SlopAndOrder) {
  // Add documents
  AddDocument({"foo", "bar"});
  AddDocument({"foo", "baz"});
  AddDocument({"bar", "foo"});
  AddDocument({"foo", "baz", "bar"});

  // Create an intersection iterator with slop and in-order
  CreateIntersectionIterator({"foo", "bar"}, 0, true);
  ASSERT_EQ(ii_base->type, INTERSECT_ITERATOR);
  ASSERT_EQ(ii_base->NumEstimated(ii_base), 3); // 3 documents match "bar"

  // Read the results. Expected: 1 (slop 0, in order)
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF); // Reading after EOF should return EOF again

  // Rewind and check again with SkipTo
  ii_base->Rewind(ii_base);
  ASSERT_EQ(ii_base->lastDocId, 0);
  ASSERT_FALSE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_OK);
  ASSERT_EQ(ii_base->current->docId, 1);
  ASSERT_EQ(ii_base->lastDocId, 1);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 2), ITERATOR_EOF);
  ASSERT_EQ(ii_base->lastDocId, 1); // Last docId should remain unchanged
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 3), ITERATOR_EOF); // Skipping beyond the last docId should return EOF
  ASSERT_EQ(ii_base->lastDocId, 1); // Last docId should remain unchanged
  ASSERT_TRUE(ii_base->atEOF); // atEOF should remain true
}


class IntersectionIteratorReducerTest : public ::testing::Test {};

TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithEmptyChild) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 3);
  children[0] = (QueryIterator *) new MockIterator({1UL, 2UL, 3UL});
  children[1] = NewEmptyIterator();
  children[2] = (QueryIterator *) new MockIterator({1UL, 2UL, 3UL, 4UL, 5UL});

  size_t num = 3;
  QueryIterator *ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);

  // Should return an empty iterator when any child is empty
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ii_base->Free(ii_base);
}

TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithNULLChild) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 3);
  children[0] = (QueryIterator *) new MockIterator({1UL, 2UL, 3UL});
  children[1] = NULL;
  children[2] = (QueryIterator *) new MockIterator({1UL, 2UL, 3UL, 4UL, 5UL});

  size_t num = 3;
  QueryIterator *ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);

  // Should return an empty iterator when any child is empty
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ii_base->Free(ii_base);
}

TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithNoChild) {
  QueryIterator *ii_base;
  size_t num = 0;

  // Test with zero children, but allocated children array
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *));
  ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);
  children = NULL; // Lose pointer to children array to ensure it is freed inside the function and does not leak

  // Should return an empty iterator when no children are provided
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ii_base->Free(ii_base);

  // Test with zero children and NULL children array
  ii_base = NewIntersectionIterator(NULL, num, -1, false, 1.0);

  // Should return an empty iterator when no children are provided
  ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
  ii_base->Free(ii_base);
}

TEST_F(IntersectionIteratorReducerTest, TestIntersectionRemovesWildcardChildren) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[1] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[2] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  // Create a READER Iterator and set the `isWildCard` flag so that it is removed by the reducer
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
  QueryIterator *iterator = NewInvIndIterator_TermQuery(idx, nullptr, {.mask_tag = FieldMaskOrIndex_Mask, .mask = 2}, nullptr, 1.0);
  InvIndIterator* invIdxIt = (InvIndIterator *)iterator;
  invIdxIt->isWildcard = true;
  children[3] = iterator;

  size_t num = 4;
  QueryIterator *ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);

  // Should remove wildcard iterators and keep only the other iterators
  ASSERT_EQ(ii_base->type, INTERSECT_ITERATOR);
  IntersectionIterator *ii = (IntersectionIterator *)ii_base;
  ASSERT_EQ(ii->num_its, 2);

  ii_base->Free(ii_base);
  InvertedIndex_Free(idx);
}

TEST_F(IntersectionIteratorReducerTest, TestIntersectionAllWildCardChildren) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[1] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[2] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[3] = NewWildcardIterator_NonOptimized(30, 2, 1.0);

  QueryIterator *expected_iter = children[3];
  size_t num = 4;
  QueryIterator *ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);
  ASSERT_EQ(ii_base, expected_iter);
  ii_base->Free(ii_base);
}

TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithSingleChild) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 3);
  children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[1] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[2] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  auto expected_type = children[0]->type;

  size_t num = 3;
  QueryIterator *ii_base = NewIntersectionIterator(children, num, -1, false, 1.0);

  ASSERT_EQ(ii_base->type, expected_type);
  ii_base->Free(ii_base);
}

// Test class for Revalidate functionality of Intersection Iterator
class IntersectionIteratorRevalidateTest : public ::testing::Test {
protected:
  QueryIterator *ii_base;
  std::vector<MockIterator*> mockChildren;
  std::vector<t_docId> commonDocIds;

  void SetUp() override {
    // Create common document IDs that all children will have
    commonDocIds = {10, 20, 30, 40, 50};

    // Create 3 mock children with overlapping doc IDs
    mockChildren.resize(3);
    auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 3);

    // Child 0: has common docs + some unique ones
    mockChildren[0] = new MockIterator({10UL, 15UL, 20UL, 25UL, 30UL, 35UL, 40UL, 45UL, 50UL, 55UL});
    children[0] = reinterpret_cast<QueryIterator *>(mockChildren[0]);

    // Child 1: has common docs + different unique ones
    mockChildren[1] = new MockIterator({5UL, 10UL, 18UL, 20UL, 28UL, 30UL, 38UL, 40UL, 48UL, 50UL, 60UL});
    children[1] = reinterpret_cast<QueryIterator *>(mockChildren[1]);

    // Child 2: has common docs + different unique ones
    mockChildren[2] = new MockIterator({2UL, 10UL, 12UL, 20UL, 22UL, 30UL, 32UL, 40UL, 42UL, 50UL, 70UL});
    children[2] = reinterpret_cast<QueryIterator *>(mockChildren[2]);

    // Create intersection iterator
    ii_base = NewIntersectionIterator(children, 3, -1, false, 1.0);
  }

  void TearDown() override {
    if (ii_base) {
      ii_base->Free(ii_base);
    }
  }
};

TEST_F(IntersectionIteratorRevalidateTest, RevalidateOK) {
  // All children return VALIDATE_OK
  for (auto& child : mockChildren) {
    child->SetRevalidateResult(VALIDATE_OK);
  }

  // Read a few documents first
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 10);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 20);

  // Revalidate should return VALIDATE_OK
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify all children were revalidated
  for (auto& child : mockChildren) {
    ASSERT_EQ(child->GetValidationCount(), 1);
  }

  // Should be able to continue reading
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 30);
}

TEST_F(IntersectionIteratorRevalidateTest, RevalidateAborted) {
  // One child returns VALIDATE_ABORTED
  mockChildren[0]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[1]->SetRevalidateResult(VALIDATE_ABORTED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Read a document first
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);

  // Revalidate should return VALIDATE_ABORTED since one child is aborted
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_ABORTED);
}

TEST_F(IntersectionIteratorRevalidateTest, RevalidateMoved) {
  // All children return VALIDATE_MOVED - each will advance by one document
  mockChildren[0]->SetRevalidateResult(VALIDATE_MOVED);
  mockChildren[1]->SetRevalidateResult(VALIDATE_MOVED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_MOVED);

  // Read a document first
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 10);

  // Revalidate should return VALIDATE_MOVED
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_EQ(ii_base->lastDocId, 20) << "After revalidation with VALIDATE_MOVED, the lastDocId should be advanced to the next common doc ID";
}

TEST_F(IntersectionIteratorRevalidateTest, RevalidateMixedResults) {
  // Mix of different revalidate results
  mockChildren[0]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[1]->SetRevalidateResult(VALIDATE_MOVED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Read a document first
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 10);

  // Revalidate should return VALIDATE_MOVED (if any child moved)
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_EQ(ii_base->lastDocId, 20);

  ASSERT_EQ(ii_base->SkipTo(ii_base, commonDocIds.back()), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 50);

  // Revalidate should return VALIDATE_MOVED, but atEOF should be true
  status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
}

TEST_F(IntersectionIteratorRevalidateTest, RevalidateAfterEOF) {
  // Test case 1: Revalidate after EOF - make sure all children were revalidated regardless, and returned OK

  // First, advance intersection iterator to EOF
  IteratorStatus rc = ii_base->SkipTo(ii_base, commonDocIds.back() + 1);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ii_base->atEOF);

  // Set all children to return VALIDATE_MOVED
  for (auto& child : mockChildren) {
    child->SetRevalidateResult(VALIDATE_MOVED);
  }

  // Revalidate should return VALIDATE_OK when already at EOF
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify all children were revalidated regardless of iterator being at EOF
  for (auto& child : mockChildren) {
    ASSERT_EQ(child->GetValidationCount(), 1) << "All children should be revalidated even when iterator is at EOF";
  }

  // Iterator should still be at EOF
  ASSERT_TRUE(ii_base->atEOF);
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
}

TEST_F(IntersectionIteratorRevalidateTest, RevalidateSomeChildrenMovedToEOF) {
  // Test case 2: Some children (at least one) moved to EOF while revalidating

  // Position intersection iterator at a valid document
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
  ASSERT_EQ(ii_base->lastDocId, 10);

  // Simulate some children moving to EOF during validation
  // Child 0: stays valid
  mockChildren[0]->SetRevalidateResult(VALIDATE_OK);

  // Child 1: moves to EOF
  mockChildren[1]->base.atEOF = true;
  mockChildren[1]->nextIndex = mockChildren[1]->docIds.size(); // Set to end
  mockChildren[1]->SetRevalidateResult(VALIDATE_MOVED);

  // Child 2: stays valid
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Revalidate should return VALIDATE_MOVED and set intersection to EOF
  // because if any child is at EOF, the intersection has no more results
  ValidateStatus status = ii_base->Revalidate(ii_base);
  ASSERT_EQ(status, VALIDATE_MOVED);

  // Intersection iterator should now be at EOF since one child moved to EOF
  ASSERT_TRUE(ii_base->atEOF);

  // Verify all children were revalidated
  for (auto& child : mockChildren) {
    ASSERT_EQ(child->GetValidationCount(), 1);
  }

  // Further reads should return EOF
  ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
  ASSERT_EQ(ii_base->SkipTo(ii_base, 100), ITERATOR_EOF);
}
