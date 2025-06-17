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
#include "src/inverted_index.h"
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
            children[i] = NewInvIndIterator_TermQuery(invertedIndexes[terms[i]], NULL, {true, RS_FIELDMASK_ALL}, NULL, 1.0);
        }
        ii_base = NewIntersectionIterator(children, terms.size(), max_slop, in_order, 1.0);
    }
    void AddDocument(const std::vector<std::string> &terms) {
        size_t dummy;
        for (auto &term : terms) {
            if (invertedIndexes.find(term) == invertedIndexes.end()) {
                // Create a new inverted index for the term if it doesn't exist
                invertedIndexes[term] = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &dummy);
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
            IndexEncoder enc = InvertedIndex_GetEncoder(index->flags);
            InvertedIndex_WriteForwardIndexEntry(index, enc, &entry);
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
    ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
    ASSERT_TRUE(ii_base->atEOF);
    ASSERT_EQ(ii_base->NumEstimated(ii_base), 0);
    ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_EOF);
    ASSERT_TRUE(ii_base->atEOF);
    ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
    ii_base->Free(ii_base);

    children = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
    children[0] = reinterpret_cast<QueryIterator *>(new MockIterator({1UL, 2UL, 3UL}));
    children[1] = nullptr;
    ii_base = NewIntersectionIterator(children, 2, -1, false, 1.0);
    ASSERT_EQ(ii_base->Read(ii_base), ITERATOR_EOF);
    ASSERT_TRUE(ii_base->atEOF);
    ASSERT_EQ(ii_base->NumEstimated(ii_base), 0);
    ASSERT_EQ(ii_base->SkipTo(ii_base, 1), ITERATOR_EOF);
    ASSERT_TRUE(ii_base->atEOF);
    ASSERT_EQ(ii_base->type, EMPTY_ITERATOR);
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
