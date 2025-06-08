/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "gtest/gtest.h"
#include "iterator_util.h"

#include "src/forward_index.h"
#include "src/iterators/inverted_index_iterator.h"

typedef enum IndexType {
    INDEX_TYPE_TERM_FULL,
    INDEX_TYPE_NUMERIC_FULL,
    INDEX_TYPE_TERM,
    INDEX_TYPE_NUMERIC,
    INDEX_TYPE_GENERIC,
} IndexType;

class IndexIteratorTest : public ::testing::TestWithParam<IndexType> {
protected:
    static constexpr size_t n_docs = 2.45 * std::max(INDEX_BLOCK_SIZE, INDEX_BLOCK_SIZE_DOCID_ONLY);
    std::array<t_docId, n_docs> resultSet;
    InvertedIndex *idx;
    QueryIterator *it_base;

    void SetUp() override {
        // Generate a set of document IDs for testing
        for (size_t i = 0; i < n_docs; ++i) {
            resultSet[i] = 2 * i + 1; // Document IDs start from 1
        }

        switch (GetParam()) {
            case INDEX_TYPE_TERM_FULL:
                SetTermsInvIndex();
                it_base = NewInvIndIterator_TermFull(idx);
                break;
            case INDEX_TYPE_NUMERIC_FULL:
                SetNumericInvIndex();
                it_base = NewInvIndIterator_NumericFull(idx);
                break;
            case INDEX_TYPE_TERM:
                SetTermsInvIndex();
                it_base = NewInvIndIterator_TermQuery(idx, nullptr, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
                break;
            case INDEX_TYPE_NUMERIC: {
                SetNumericInvIndex();
                FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
                FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
                it_base = NewInvIndIterator_NumericQuery(idx, nullptr, &fieldCtx, nullptr, -INFINITY, INFINITY);
            }
                break;
            case INDEX_TYPE_GENERIC:
                SetGenericInvIndex();
                it_base = NewInvIndIterator_GenericQuery(idx, nullptr, 0, FIELD_EXPIRATION_DEFAULT);
                break;
        }
    }
    void TearDown() override {
        it_base->Free(it_base);
        InvertedIndex_Free(idx);
    }

private:
    void SetTermsInvIndex() {
        // This function should populate the InvertedIndex with terms
        size_t memsize;
        idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &memsize);
        IndexEncoder encoder = InvertedIndex_GetEncoder(idx->flags);
        RS_ASSERT_ALWAYS(InvertedIndex_GetDecoder(idx->flags).seeker != nullptr); // Expect a seeker with the default flags
        for (size_t i = 0; i < n_docs; ++i) {
            ForwardIndexEntry h = {0};
            h.docId = resultSet[i];
            h.fieldMask = i + 1;
            h.freq = i + 1;
            h.term = "term";
            h.len = 4; // Length of the term "term"

            h.vw = NewVarintVectorWriter(8);
            VVW_Write(h.vw, i); // Just writing the index as a value
            InvertedIndex_WriteForwardIndexEntry(idx, encoder, &h);
            VVW_Free(h.vw);
        }
    }

    void SetNumericInvIndex() {
        // This function should populate the InvertedIndex with numeric data
        size_t memsize;
        idx = NewInvertedIndex(Index_StoreNumeric, 1, &memsize);
        for (size_t i = 0; i < n_docs; ++i) {
            InvertedIndex_WriteNumericEntry(idx, resultSet[i], static_cast<double>(i));
        }
    }

    void SetGenericInvIndex() {
        // This function should populate the InvertedIndex with generic data
        size_t memsize;
        idx = NewInvertedIndex(Index_DocIdsOnly, 1, &memsize);
        IndexEncoder encoder = InvertedIndex_GetEncoder(idx->flags);
        for (size_t i = 0; i < n_docs; ++i) {
            InvertedIndex_WriteEntryGeneric(idx, encoder, resultSet[i], nullptr);
        }
    }
};


INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTest, ::testing::Values(
    INDEX_TYPE_TERM_FULL,
    INDEX_TYPE_NUMERIC_FULL,
    INDEX_TYPE_TERM,
    INDEX_TYPE_NUMERIC,
    INDEX_TYPE_GENERIC
));


TEST_P(IndexIteratorTest, Read) {
    InvIndIterator *it = (InvIndIterator *)it_base;
    IteratorStatus rc;

    // Test reading until EOF
    size_t i = 0;
    while ((rc = it_base->Read(it_base)) == ITERATOR_OK) {
        ASSERT_EQ(it->base.current->docId, resultSet[i]);
        ASSERT_EQ(it->base.lastDocId, resultSet[i]);
        ASSERT_FALSE(it->base.atEOF);
        i++;
    }
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_TRUE(it->base.atEOF);
    ASSERT_EQ(it_base->Read(it_base), ITERATOR_EOF); // Reading after EOF should return EOF
    ASSERT_EQ(i, resultSet.size()) << "Expected to read " << resultSet.size() << " documents";
    ASSERT_EQ(it_base->NumEstimated(it_base), resultSet.size());
    ASSERT_EQ(it_base->NumEstimated(it_base), idx->numDocs);
}

TEST_P(IndexIteratorTest, SkipTo) {
    InvIndIterator *it = (InvIndIterator *)it_base;
    IteratorStatus rc;
    // Test skipping to any id between 1 and the last id
    t_docId i = 1;
    for (t_docId id : resultSet) {
        while (i < id) {
            it_base->Rewind(it_base);
            rc = it_base->SkipTo(it_base, i);
            ASSERT_EQ(rc, ITERATOR_NOTFOUND);
            ASSERT_EQ(it->base.lastDocId, id);
            ASSERT_EQ(it->base.current->docId, id);
            i++;
        }
        it_base->Rewind(it_base);
        rc = it_base->SkipTo(it_base, id);
        ASSERT_EQ(rc, ITERATOR_OK);
        ASSERT_EQ(it->base.lastDocId, id);
        ASSERT_EQ(it->base.current->docId, id);
        i++;
    }
    // Test reading after skipping to the last id
    ASSERT_EQ(it_base->Read(it_base), ITERATOR_EOF);
    ASSERT_EQ(it_base->SkipTo(it_base, it_base->lastDocId + 1), ITERATOR_EOF);
    ASSERT_TRUE(it->base.atEOF);

    it_base->Rewind(it_base);
    ASSERT_EQ(it->base.lastDocId, 0);
    ASSERT_FALSE(it->base.atEOF);
    // Test skipping to all ids that exist
    for (t_docId id : resultSet) {
        rc = it_base->SkipTo(it_base, id);
        ASSERT_EQ(rc, ITERATOR_OK);
        ASSERT_EQ(it->base.lastDocId, id);
        ASSERT_EQ(it->base.current->docId, id);
    }

    // Test skipping to an id that exceeds the last id
    it_base->Rewind(it_base);
    ASSERT_EQ(it->base.lastDocId, 0);
    ASSERT_FALSE(it->base.atEOF);
    rc = it_base->SkipTo(it_base, resultSet.back() + 1);
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_EQ(it->base.lastDocId, 0); // we just rewound
    ASSERT_TRUE(it->base.atEOF);
}
