/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rmutil/alloc.h"

#include "gtest/gtest.h"
#include "iterator_util.h"

#include "src/iterators/intersection_iterator.h"

class IntersectionIteratorCommonTest : public ::testing::TestWithParam<std::tuple<unsigned, std::vector<t_docId>>> {
protected:
    std::vector<std::vector<t_docId>> docIds;
    std::vector<t_docId> resultSet;
    QueryIterator *ii_base;

    void SetUp() override {
        unsigned numChildren;
        std::tie(numChildren, resultSet) = GetParam();
        // Set docIds so the intersection of all children is resultSet.
        // Make sure that some ids are unique to each child
        docIds.resize(numChildren);
        t_docId id = 1;
        for (auto &childIds : docIds) {
            // Copy the resultSet to each child as a base
            childIds = resultSet;
            // Add some unique ids to each child. Mock constructors will ensure that the ids are unique and sorted.
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

// class IntersectionIteratorEdgesTest : public ::testing::TestWithParam<std::tuple<unsigned, bool, bool>> {
// protected:
//     QueryIterator *ii_base;

//     void SetUp() override {
//         auto [numChildren, quickExit, sparse_ids] = GetParam();
//         auto children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * numChildren);
//         for (unsigned i = 0; i < numChildren; i++) {
//             MockIterator *it;
//             if (sparse_ids) {
//                 it = new MockIterator(10UL, 20UL, 30UL, 40UL, 50UL);
//             } else {
//                 it = new MockIterator(1UL, 2UL, 3UL, 4UL, 5UL);
//             }
//             children[i] = (QueryIterator *) it;
//         }
//         // Create a union iterator
//         ii_base = IT_V2(NewIntersectionIterator)(children, numChildren, quickExit, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
//     }
//     void TearDown() override {
//         ii_base->Free(ii_base);
//     }

//     void TimeoutChildTest(int childIdx) {
//         IntersectionIterator *ii = (IntersectionIterator *)ii_base;
//         auto [numChildren, quickExit, sparse_ids] = GetParam();

//         auto child = reinterpret_cast<MockIterator *>(ii->its[childIdx]);
//         child->whenDone = ITERATOR_TIMEOUT;
//         child->docIds.clear();

//         auto rc = ii_base->Read(ii_base);
//         if (!quickExit || sparse_ids) {
//             // Usually, the first read will detect the timeout
//             ASSERT_EQ(rc, ITERATOR_TIMEOUT);
//         } else {
//             // If quickExit is enabled and we have a dense range of ids, we may not read from the timed-out child
//             ASSERT_TRUE(rc == ITERATOR_OK || rc == ITERATOR_TIMEOUT);
//             // We still expect the first non-ok status to be TIMEOUT
//             while (rc == ITERATOR_OK) {
//                 rc = ii_base->Read(ii_base);
//             }
//             ASSERT_EQ(rc, ITERATOR_TIMEOUT);
//         }

//         ii_base->Rewind(ii_base);

//         // Test skipping with a timeout child
//         t_docId next = 1;
//         rc = ii_base->SkipTo(ii_base, next);
//         if (!quickExit || sparse_ids) {
//             // Usually, the first read will detect the timeout
//             ASSERT_EQ(rc, ITERATOR_TIMEOUT);
//         } else {
//             // If quickExit is enabled and we have a dense range of ids, we may not read from the timed-out child
//             ASSERT_TRUE(rc == ITERATOR_OK || rc == ITERATOR_TIMEOUT);
//             // We still expect the first non-ok status to be TIMEOUT
//             while (rc == ITERATOR_OK) {
//                 rc = ii_base->SkipTo(ii_base, ++next);
//             }
//             ASSERT_EQ(rc, ITERATOR_TIMEOUT);
//         }
//     }
// };

// // Run the test in the case where the first child times out
// TEST_P(IntersectionIteratorEdgesTest, TimeoutFirstChild) {
//     TimeoutChildTest(0);
// }

// // Run the test in the case where some middle child times out
// TEST_P(IntersectionIteratorEdgesTest, TimeoutMidChild) {
//     TimeoutChildTest(std::get<0>(GetParam()) / 2);
// }

// // Run the test in the case where the last child times out
// TEST_P(IntersectionIteratorEdgesTest, TimeoutLastChild) {
//     TimeoutChildTest(std::get<0>(GetParam()) - 1);
// }

// // Parameters for the tests above. We run all the combinations of:
// // 1. number of child iterators in {2, 5, 25}
// // 2. quick mode (true/false)
// // 3. sparse/dense result set (we may get different behavior if we have sequential ids to return)
// INSTANTIATE_TEST_SUITE_P(IntersectionIterator, IntersectionIteratorEdgesTest, ::testing::Combine(
//     ::testing::Values(2, 5, 25),
//     ::testing::Bool(),
//     ::testing::Bool()
// ));

// class IntersectionIteratorSingleTest : public ::testing::Test {};

// TEST_F(IntersectionIteratorSingleTest, ReuseResults) {
//     QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 2);
//     MockIterator *it1 = new MockIterator(3UL);
//     MockIterator *it2 = new MockIterator(2UL);
//     children[0] = (QueryIterator *)it1;
//     children[1] = (QueryIterator *)it2;
//     // Create a union iterator
//     IteratorsConfig config = RSGlobalConfig.iteratorsConfigParams;
//     config.minUnionIterHeap = INT64_MAX; // Ensure we don't use the heap
//     QueryIterator *ii_base = IT_V2(NewIntersectionIterator)(children, 2, true, 1.0, QN_UNION, NULL, &config);
//     ASSERT_EQ(ii_base->NumEstimated(ii_base), it1->docIds.size() + it2->docIds.size());

//     ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
//     ASSERT_EQ(ii_base->lastDocId, 2);
//     ASSERT_EQ(it1->base.lastDocId, 3);
//     ASSERT_EQ(it2->base.lastDocId, 2);
//     ASSERT_EQ(it1->readCount, 1);
//     ASSERT_EQ(it2->readCount, 1);

//     ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_OK);
//     ASSERT_EQ(ii_base->lastDocId, 3);
//     ASSERT_EQ(it1->base.lastDocId, 3);
//     ASSERT_EQ(it2->base.lastDocId, 2);
//     ASSERT_EQ(it1->readCount, 1) << "it1 should not be read again";
//     ASSERT_FALSE(it1->base.atEOF);
//     ASSERT_EQ(it2->readCount, 1) << "it2 should not be read again";
//     ASSERT_FALSE(it2->base.atEOF);

//     ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
//     ASSERT_EQ(it1->readCount, 2) << "it1 should be read again";
//     ASSERT_TRUE(it1->base.atEOF);
//     ASSERT_EQ(it2->readCount, 2) << "it2 should be read again";
//     ASSERT_TRUE(it2->base.atEOF);

//     ii_base->Free(ii_base);
// }
