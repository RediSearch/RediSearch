/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rmutil/alloc.h"

#include "gtest/gtest.h"
#include "iterator_util.h"

#include "src/iterators/union_iterator.h"
#include "src/iterators/empty_iterator.h"
#include "src/iterators/wildcard_iterator.h"
#include "src/iterators/inverted_index_iterator.h"
#include "inverted_index.h"

class UnionIteratorCommonTest : public ::testing::TestWithParam<std::tuple<unsigned, bool, std::vector<t_docId>>> {
protected:
  std::vector<std::vector<t_docId>> docIds;
  std::vector<t_docId> resultSet;
  QueryIterator *ui_base;

  void SetUp() override {
    ASSERT_EQ(RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap, 20) <<
        "If we ever change the default threshold for using heaps, we need to modify the tests "
        "here so they still check both flat and heap alternatives.";

    auto [numChildren, quickExit, union_res] = GetParam();
    // Set resultSet to the expected union result
    resultSet = union_res;
    // Set docIds so the union of all children is union_res.
    // Make sure that some ids are repeated in some children
    docIds.resize(numChildren);
    for (size_t i = 0; i < union_res.size(); i++) {
      for (unsigned j = 0; j < numChildren; j++) {
        if (j % (i + 1) == 0) {
          docIds[j].push_back(union_res[i]);
        }
      }
    }
    // Create children iterators
    auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * numChildren);
    for (unsigned i = 0; i < numChildren; i++) {
      children[i] = (QueryIterator *) new MockIterator(docIds[i]);
    }
    // Create a union iterator
    ui_base = NewUnionIterator(children, numChildren, quickExit, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  }
  void TearDown() override {
    ui_base->Free(ui_base);
  }
};

TEST_P(UnionIteratorCommonTest, Read) {
  UnionIterator *ui = (UnionIterator *)ui_base;
  IteratorStatus rc;

  // Test reading until EOF
  size_t i = 0;
  while ((rc = ui_base->Read(ui_base)) == ITERATOR_OK) {
    ASSERT_EQ(ui->base.current->docId, resultSet[i]);
    ASSERT_EQ(ui->base.lastDocId, resultSet[i]);
    ASSERT_FALSE(ui->base.atEOF);
    i++;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ui->base.atEOF);
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(i, resultSet.size()) << "Expected to read " << resultSet.size() << " documents";

  size_t expected = 0;
  for (auto &child : docIds) {
    expected += child.size();
  }
  ASSERT_EQ(ui_base->NumEstimated(ui_base), expected);
}

TEST_P(UnionIteratorCommonTest, SkipTo) {
    UnionIterator *ui = (UnionIterator *)ui_base;
    IteratorStatus rc;
    // Test skipping to any id between 1 and the last id
    t_docId i = 1;
    for (t_docId id : resultSet) {
        while (i < id) {
            ui_base->Rewind(ui_base);
            rc = ui_base->SkipTo(ui_base, i);
            ASSERT_EQ(rc, ITERATOR_NOTFOUND);
            ASSERT_EQ(ui->base.lastDocId, id);
            ASSERT_EQ(ui->base.current->docId, id);
            i++;
        }
        ui_base->Rewind(ui_base);
        rc = ui_base->SkipTo(ui_base, id);
        ASSERT_EQ(rc, ITERATOR_OK);
        ASSERT_EQ(ui->base.lastDocId, id);
        ASSERT_EQ(ui->base.current->docId, id);
        i++;
    }
    // Test reading after skipping to the last id
    ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_EOF);
    ASSERT_EQ(ui_base->SkipTo(ui_base, ui_base->lastDocId + 1), ITERATOR_EOF);
    ASSERT_TRUE(ui->base.atEOF);

    ui_base->Rewind(ui_base);
    ASSERT_EQ(ui->base.lastDocId, 0);
    ASSERT_FALSE(ui->base.atEOF);
    // Test skipping to all ids that exist
    for (t_docId id : resultSet) {
        rc = ui_base->SkipTo(ui_base, id);
        ASSERT_EQ(rc, ITERATOR_OK);
        ASSERT_EQ(ui->base.lastDocId, id);
        ASSERT_EQ(ui->base.current->docId, id);
    }

    // Test skipping to an id that exceeds the last id
    ui_base->Rewind(ui_base);
    ASSERT_EQ(ui->base.lastDocId, 0);
    ASSERT_FALSE(ui->base.atEOF);
    rc = ui_base->SkipTo(ui_base, resultSet.back() + 1);
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_EQ(ui->base.lastDocId, 0); // we just rewound
    ASSERT_TRUE(ui->base.atEOF);
}

TEST_P(UnionIteratorCommonTest, Rewind) {
  UnionIterator *ui = (UnionIterator *)ui_base;
  IteratorStatus rc;
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j <= i; j++) {
      ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
      ASSERT_EQ(ui->base.current->docId, resultSet[j]);
      ASSERT_EQ(ui->base.lastDocId, resultSet[j]);
    }
    ui_base->Rewind(ui_base);
    ASSERT_EQ(ui->base.lastDocId, 0);
    ASSERT_FALSE(ui->base.atEOF);
  }
}

// Parameters for the tests above. We run all the combinations of:
// 1. number of child iterators in {2, 5, 25}
// 2. quick mode (true/false)
// 3. expected result set, one of the 3 given lists below
INSTANTIATE_TEST_SUITE_P(UnionIteratorP, UnionIteratorCommonTest, ::testing::Combine(
  ::testing::Values(2, 5, 25),
  ::testing::Bool(),
  ::testing::Values(
    std::vector<t_docId>{1, 2, 3, 40, 50},
    std::vector<t_docId>{5, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012, 12345, 23456, 34567, 45678, 56789},
    std::vector<t_docId>{9, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250}
  )
));

class UnionIteratorEdgesTest : public ::testing::TestWithParam<std::tuple<unsigned, bool, bool>> {
protected:
  QueryIterator *ui_base;

  void SetUp() override {
    auto [numChildren, quickExit, sparse_ids] = GetParam();
    auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * numChildren);
    for (unsigned i = 0; i < numChildren; i++) {
      MockIterator *it;
      if (sparse_ids) {
        it = new MockIterator(10UL, 20UL, 30UL, 40UL, 50UL);
      } else {
        it = new MockIterator(1UL, 2UL, 3UL, 4UL, 5UL);
      }
      children[i] = (QueryIterator *) it;
    }
    // Create a union iterator
    ui_base = NewUnionIterator(children, numChildren, quickExit, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  }
  void TearDown() override {
    ui_base->Free(ui_base);
  }

  void TimeoutChildTest(int childIdx) {
    UnionIterator *ui = (UnionIterator *)ui_base;
    auto [numChildren, quickExit, sparse_ids] = GetParam();

    auto child = reinterpret_cast<MockIterator *>(ui->its[childIdx]);
    child->whenDone = ITERATOR_TIMEOUT;
    child->docIds.clear();

    auto rc = ui_base->Read(ui_base);
    if (!quickExit || sparse_ids) {
      // Usually, the first read will detect the timeout
      ASSERT_EQ(rc, ITERATOR_TIMEOUT);
    } else {
      // If quickExit is enabled and we have a dense range of ids, we may not read from the timed-out child
      ASSERT_TRUE(rc == ITERATOR_OK || rc == ITERATOR_TIMEOUT);
      // We still expect the first non-ok status to be TIMEOUT
      while (rc == ITERATOR_OK) {
        rc = ui_base->Read(ui_base);
      }
      ASSERT_EQ(rc, ITERATOR_TIMEOUT);
    }

    ui_base->Rewind(ui_base);

    // Test skipping with a timeout child
    t_docId next = 1;
    rc = ui_base->SkipTo(ui_base, next);
    if (!quickExit || sparse_ids) {
      // Usually, the first read will detect the timeout
      ASSERT_EQ(rc, ITERATOR_TIMEOUT);
    } else {
      // If quickExit is enabled and we have a dense range of ids, we may not read from the timed-out child
      ASSERT_TRUE(rc == ITERATOR_OK || rc == ITERATOR_TIMEOUT);
      // We still expect the first non-ok status to be TIMEOUT
      while (rc == ITERATOR_OK) {
        rc = ui_base->SkipTo(ui_base, ++next);
      }
      ASSERT_EQ(rc, ITERATOR_TIMEOUT);
    }
  }
};

// Run the test in the case where the first child times out
TEST_P(UnionIteratorEdgesTest, TimeoutFirstChild) {
    TimeoutChildTest(0);
}

// Run the test in the case where some middle child times out
TEST_P(UnionIteratorEdgesTest, TimeoutMidChild) {
    TimeoutChildTest(std::get<0>(GetParam()) / 2);
}

// Run the test in the case where the last child times out
TEST_P(UnionIteratorEdgesTest, TimeoutLastChild) {
    TimeoutChildTest(std::get<0>(GetParam()) - 1);
}

// Parameters for the tests above. We run all the combinations of:
// 1. number of child iterators in {2, 5, 25}
// 2. quick mode (true/false)
// 3. sparse/dense result set (we may get different behavior if we have sequential ids to return)
INSTANTIATE_TEST_SUITE_P(UnionIteratorEdgesP, UnionIteratorEdgesTest, ::testing::Combine(
    ::testing::Values(2, 5, 25),
    ::testing::Bool(),
    ::testing::Bool()
));

class UnionIteratorSingleTest : public ::testing::Test {};

TEST_F(UnionIteratorSingleTest, ReuseResults) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 2);
  MockIterator *it1 = new MockIterator(3UL);
  MockIterator *it2 = new MockIterator(2UL);
  children[0] = (QueryIterator *)it1;
  children[1] = (QueryIterator *)it2;
  // Create a union iterator
  IteratorsConfig config = RSGlobalConfig.iteratorsConfigParams;
  config.minUnionIterHeap = INT64_MAX; // Ensure we don't use the heap
  QueryIterator *ui_base = NewUnionIterator(children, 2, true, 1.0, QN_UNION, NULL, &config);
  ASSERT_EQ(ui_base->NumEstimated(ui_base), it1->docIds.size() + it2->docIds.size());

  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 2);
  ASSERT_EQ(it1->base.lastDocId, 3);
  ASSERT_EQ(it2->base.lastDocId, 2);
  ASSERT_EQ(it1->readCount, 1);
  ASSERT_EQ(it2->readCount, 1);

  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 3);
  ASSERT_EQ(it1->base.lastDocId, 3);
  ASSERT_EQ(it2->base.lastDocId, 2);
  ASSERT_EQ(it1->readCount, 1) << "it1 should not be read again";
  ASSERT_FALSE(it1->base.atEOF);
  ASSERT_EQ(it2->readCount, 1) << "it2 should not be read again";
  ASSERT_FALSE(it2->base.atEOF);

  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_EOF);
  ASSERT_EQ(it1->readCount, 2) << "it1 should be read again";
  ASSERT_TRUE(it1->base.atEOF);
  ASSERT_EQ(it2->readCount, 2) << "it2 should be read again";
  ASSERT_TRUE(it2->base.atEOF);

  ui_base->Free(ui_base);
}


class UnionIteratorReducerTest : public ::testing::Test {};

TEST_F(UnionIteratorReducerTest, TestUnionRemovesEmptyChildren) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = nullptr;
  children[1] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[2] = NewEmptyIterator();
  children[3] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  QueryIterator *ui_base = NewUnionIterator(children, 4, false, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  ASSERT_EQ(ui_base->type, UNION_ITERATOR);
  UnionIterator *ui = (UnionIterator *)ui_base;
  ASSERT_EQ(ui->num, 2);
  ui_base->Free(ui_base);
}

TEST_F(UnionIteratorReducerTest, TestUnionRemovesAllEmptyChildren) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = nullptr;
  children[1] = NewEmptyIterator();
  children[2] = NewEmptyIterator();
  children[3] = nullptr;
  QueryIterator *ui_base = NewUnionIterator(children, 4, false, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  ASSERT_EQ(ui_base->type, EMPTY_ITERATOR);
  ui_base->Free(ui_base);
}

TEST_F(UnionIteratorReducerTest, TestUnionRemovesEmptyChildrenOnlyOneLeft) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = nullptr;
  children[1] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[2] = NewEmptyIterator();
  children[3] = nullptr;
  QueryIterator* expected_iter = children[1];
  QueryIterator *ui_base = NewUnionIterator(children, 4, false, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  ASSERT_EQ(ui_base, expected_iter);
  ui_base->Free(ui_base);
}

TEST_F(UnionIteratorReducerTest, TestUnionQuickWithWildcard) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
  children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[1] = NewWildcardIterator_NonOptimized(30, 2, 1.0);
  children[2] = nullptr;
  children[3] = NewEmptyIterator();
  QueryIterator *ui_base = NewUnionIterator(children, 4, true, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  ASSERT_EQ(ui_base->type, WILDCARD_ITERATOR);
  ui_base->Free(ui_base);
}

TEST_F(UnionIteratorReducerTest, TestUnionQuickWithReaderWildcard) {
  QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 4);
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
  children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
  children[1] = iterator;
  children[2] = nullptr;
  children[3] = NewEmptyIterator();
  QueryIterator *ui_base = NewUnionIterator(children, 4, true, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  ASSERT_EQ(ui_base->type, INV_IDX_ITERATOR);
  ui_base->Free(ui_base);
  InvertedIndex_Free(idx);
}

// Test class for Revalidate functionality of Union Iterator
class UnionIteratorRevalidateTest : public ::testing::Test {
protected:
  QueryIterator *ui_base;
  std::vector<MockIterator*> mockChildren;

  void SetUp() override {
    // Create 3 mock children with different doc IDs
    mockChildren.resize(3);
    auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 3);

    // Child 0: docs [10, 30, 50]
    mockChildren[0] = new MockIterator({10UL, 30UL, 50UL});
    children[0] = reinterpret_cast<QueryIterator *>(mockChildren[0]);

    // Child 1: docs [20, 40, 50, 60]
    mockChildren[1] = new MockIterator({20UL, 40UL, 50UL, 60UL});
    children[1] = reinterpret_cast<QueryIterator *>(mockChildren[1]);

    // Child 2: docs [15, 35, 55]
    mockChildren[2] = new MockIterator({15UL, 35UL, 55UL});
    children[2] = reinterpret_cast<QueryIterator *>(mockChildren[2]);

    // Create union iterator (should yield: 10, 15, 20, 30, 35, 40, 50, 55, 60)
    ui_base = NewUnionIterator(children, 3, false, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
  }

  void TearDown() override {
    if (ui_base) {
      ui_base->Free(ui_base);
    }
  }
};

TEST_F(UnionIteratorRevalidateTest, RevalidateOK) {
  // All children return VALIDATE_OK
  for (auto& child : mockChildren) {
    child->SetRevalidateResult(VALIDATE_OK);
  }

  // Read a few documents first
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 10);
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 15);
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 20);

  // Revalidate should return VALIDATE_OK
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify all children were revalidated
  for (auto& child : mockChildren) {
    ASSERT_EQ(child->GetValidationCount(), 1);
  }

  // Should be able to continue reading
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 30);
}

TEST_F(UnionIteratorRevalidateTest, RevalidateAborted) {
  // Union iterator only returns VALIDATE_ABORTED if ALL children are aborted
  // If only some children are aborted, it removes them and continues

  // Set all children to be aborted
  mockChildren[0]->SetRevalidateResult(VALIDATE_ABORTED);
  mockChildren[1]->SetRevalidateResult(VALIDATE_ABORTED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_ABORTED);

  // Read a document first
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);

  // Revalidate should return VALIDATE_ABORTED since all children are aborted
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_ABORTED);
}

TEST_F(UnionIteratorRevalidateTest, RevalidatePartiallyAborted) {
  // When only some children are aborted, Union iterator removes them and continues
  mockChildren[0]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[1]->SetRevalidateResult(VALIDATE_ABORTED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Read a document first
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  t_docId docIdBeforeRevalidate = ui_base->lastDocId;
  ASSERT_EQ(docIdBeforeRevalidate, 10);

  // Revalidate should return VALIDATE_OK or VALIDATE_MOVED (not ABORTED)
  // since not all children are aborted
  ASSERT_EQ(mockChildren[0]->base.lastDocId, ui_base->lastDocId); // Child with a matching doc ID is OK
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Should be able to continue reading after removing aborted child
  IteratorStatus read_status = ui_base->Read(ui_base);
  ASSERT_EQ(read_status, ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 15); // Should read the next valid doc ID
}

TEST_F(UnionIteratorRevalidateTest, RevalidateMoved) {
  // All children return VALIDATE_MOVED - each will advance by one document
  mockChildren[0]->SetRevalidateResult(VALIDATE_MOVED);
  mockChildren[1]->SetRevalidateResult(VALIDATE_MOVED);
  mockChildren[2]->SetRevalidateResult(VALIDATE_MOVED);

  // Read a document first
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 10);

  // Revalidate should return VALIDATE_MOVED
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_EQ(ui_base->lastDocId, 30); // Last doc ID should have moved forward (to the minimum of the second result of each child)

  // Edge case: child returns VALIDATE_OK due to EOF, its current result is invalid
  ASSERT_EQ(ui_base->SkipTo(ui_base, 40), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 40);
  // Revalidate should still return VALIDATE_MOVED since we moved to a valid result
  status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_MOVED);
  ASSERT_EQ(ui_base->lastDocId, 50); // Should have moved to the next valid result
  ASSERT_FALSE(ui_base->atEOF); // Still not at EOF
}

TEST_F(UnionIteratorRevalidateTest, RevalidateChildAtEOFBeforeValidation) {
  // Scenario 1: A child is at EOF before the revalidation

  // Read all documents from child 0 first to bring it to EOF
  while (mockChildren[0]->base.Read(&mockChildren[0]->base) == ITERATOR_OK) {
    // Continue reading until EOF
  }
  ASSERT_TRUE(mockChildren[0]->base.atEOF);

  // Set revalidate results: child 0 (at EOF) returns OK, others return OK
  mockChildren[0]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[1]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Position union iterator at first document
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 15); // Should be 15 since child 0 is at EOF

  // Revalidate should return VALIDATE_OK since no changes occurred
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Verify union iterator continues to work correctly with remaining children
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 20);
}

TEST_F(UnionIteratorRevalidateTest, RevalidateChildMovesToEOFDuringValidation) {
  // Scenario 2: A child moves to EOF due to the validation

  // Position union iterator and children properly
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 10);

  // Simulate child 0 moving to EOF during validation
  // We'll manually set it to EOF and configure VALIDATE_MOVED
  mockChildren[0]->base.atEOF = true;
  mockChildren[0]->nextIndex = mockChildren[0]->docIds.size(); // Set to end
  mockChildren[0]->SetRevalidateResult(VALIDATE_MOVED);

  // Other children remain valid
  mockChildren[1]->SetRevalidateResult(VALIDATE_OK);
  mockChildren[2]->SetRevalidateResult(VALIDATE_OK);

  // Revalidate should return VALIDATE_MOVED since one child moved and affected the union state
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_MOVED);

  // The union should now show the next minimum docId from remaining active children
  // Since child 0 is at EOF, minimum should be from children 1 and 2
  ASSERT_EQ(ui_base->lastDocId, 15); // Next available doc from remaining children

  // Verify union iterator continues to work correctly
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_OK);
  ASSERT_EQ(ui_base->lastDocId, 20);
}

TEST_F(UnionIteratorRevalidateTest, RevalidateAllChildrenAtEOFAfterValidation) {
  // Scenario 3: All children are at EOF after the validation

  // First, advance union iterator near the end
  IteratorStatus rc;
  t_docId lastValidDocId = 0;
  while ((rc = ui_base->Read(ui_base)) == ITERATOR_OK) {
    lastValidDocId = ui_base->lastDocId;
  }
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(ui_base->atEOF);
  ASSERT_EQ(lastValidDocId, 60); // Should be the last document

  // Manually reset EOF state to test the scenario where children move to EOF during validation
  ui_base->atEOF = false;
  ui_base->lastDocId = lastValidDocId;

  // Set all children to EOF during validation
  for (auto& child : mockChildren) {
    child->base.atEOF = true;
    child->nextIndex = child->docIds.size(); // Set to end
    child->SetRevalidateResult(VALIDATE_MOVED);
  }

  // When all children are at EOF after validation, the union iterator's lastDocId won't change
  // because there are no active children to provide a new minimum docId.
  // Therefore, it should return VALIDATE_OK (not VALIDATE_MOVED) since the lastDocId is unchanged.
  ValidateStatus status = ui_base->Revalidate(ui_base);
  ASSERT_EQ(status, VALIDATE_OK);

  // Union iterator should now be at EOF since all children are at EOF
  ASSERT_TRUE(ui_base->atEOF);

  // The lastDocId should remain unchanged at the last valid document
  ASSERT_EQ(ui_base->lastDocId, lastValidDocId);

  // Further reads should return EOF
  ASSERT_EQ(ui_base->Read(ui_base), ITERATOR_EOF);
  ASSERT_EQ(ui_base->SkipTo(ui_base, lastValidDocId + 1), ITERATOR_EOF);
}
