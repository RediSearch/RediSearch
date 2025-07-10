/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "gtest/gtest.h"
#include "iterator_util.h"

#include "src/forward_index.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/tag_index.h"

typedef enum IndexType {
    INDEX_TYPE_TERM_FULL,
    INDEX_TYPE_NUMERIC_FULL,
    INDEX_TYPE_TERM,
    INDEX_TYPE_NUMERIC,
    INDEX_TYPE_GENERIC,
    INDEX_TYPE_TAG_FULL,
    INDEX_TYPE_TAG,
} IndexType;

class IndexIteratorTest : public ::testing::TestWithParam<IndexType> {
protected:
    static constexpr size_t n_docs = 2.45 * std::max(INDEX_BLOCK_SIZE, INDEX_BLOCK_SIZE_DOCID_ONLY);
    std::array<t_docId, n_docs> resultSet;
    InvertedIndex *idx;
    QueryIterator *it_base;
    TagIndex *tagIdx;

    void SetUp() override {
        // Initialize TagIndex to nullptr
        tagIdx = nullptr;

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
            case INDEX_TYPE_TAG_FULL:
                SetTagInvIndex();
                it_base = NewInvIndIterator_TagFull(idx, tagIdx);
                break;
            case INDEX_TYPE_TAG: {
                SetTagInvIndex();
                RSToken tok = {.str = (char *)"tag", .len = 3};
                RSQueryTerm *term = NewQueryTerm(&tok, 0);
                FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
                it_base = NewInvIndIterator_TagQuery(idx, tagIdx, nullptr, fieldMaskOrIndex, term, 1.0);
            }
                break;
            case INDEX_TYPE_GENERIC:
                SetGenericInvIndex();
                it_base = NewInvIndIterator_GenericQuery(idx, nullptr, 0, FIELD_EXPIRATION_DEFAULT, 1.0);
                break;

        }
    }
    void TearDown() override {
        it_base->Free(it_base);
        // TagIndex owns the inverted index
        if (!tagIdx) {
            InvertedIndex_Free(idx);
        }
        if (tagIdx) {
            TagIndex_Free(tagIdx);
        }
    }

private:
    void SetTermsInvIndex() {
        // This function should populate the InvertedIndex with terms
        size_t memsize;
        idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &memsize);
        IndexEncoder encoder = InvertedIndex_GetEncoder(idx->flags);
        ASSERT_TRUE(InvertedIndex_GetDecoder(idx->flags).seeker != nullptr); // Expect a seeker with the default flags
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

    void SetTagInvIndex() {
        // This function should populate the InvertedIndex with tag data using TagIndex API
        tagIdx = NewTagIndex();
        ASSERT_TRUE(tagIdx != nullptr);

        // Index tags for each document
        for (size_t i = 0; i < n_docs; ++i) {
            std::vector<const char *> tags{"tag", "test"};
            TagIndex_Index(tagIdx, &tags[0], tags.size(), resultSet[i]);
        }

        // Get the inverted index for the "tag" value from the TagIndex
        size_t sz;
        idx = TagIndex_OpenIndex(tagIdx, "tag", 3, false, &sz);
        ASSERT_TRUE(idx != nullptr);
        ASSERT_NE(idx, TRIEMAP_NOTFOUND);
    }
};


INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTest, ::testing::Values(
    INDEX_TYPE_TERM_FULL,
    INDEX_TYPE_NUMERIC_FULL,
    INDEX_TYPE_TERM,
    INDEX_TYPE_NUMERIC,
    INDEX_TYPE_GENERIC,
    INDEX_TYPE_TAG_FULL,
    INDEX_TYPE_TAG
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


class IndexIteratorTestEdges : public ::testing::Test {
protected:
    InvertedIndex *idx;
    QueryIterator *iterator;
    NumericFilter *flt;

    void SetUp() override {
        size_t memsize;
        idx = NewInvertedIndex(Index_StoreNumeric, 1, &memsize);
        ASSERT_TRUE(idx != nullptr);
        iterator = nullptr;
        flt = nullptr;
    }

    void TearDown() override {
        if (flt) NumericFilter_Free(flt);
        if (iterator) iterator->Free(iterator);
        InvertedIndex_Free(idx);
    }

public:
    void AddEntry(t_docId docId, double value) {
        InvertedIndex_WriteNumericEntry(idx, docId, value);
    }
    void AddEntries(t_docId start, t_docId end, double value) {
        for (t_docId docId = start; docId < end; ++docId) {
            AddEntry(docId, value);
        }
    }
    void CreateIterator(double value) {
        CreateIterator(value, value);
    }
    void CreateIterator(double min, double max) {
        ASSERT_TRUE(idx != nullptr);
        FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
        FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
        flt = NewNumericFilter(min, max, 1, 1, 1, nullptr);
        iterator = NewInvIndIterator_NumericQuery(idx, nullptr, &fieldCtx, flt, min, max);
        ASSERT_TRUE(iterator != nullptr);
    }
};

TEST_F(IndexIteratorTestEdges, SkipMultiValues) {
    // Add multiple entries with the same docId
    AddEntry(1, 1.0);
    AddEntry(1, 2.0);
    AddEntry(1, 3.0);
    CreateIterator(1.0, 3.0);

    // Read the first entry. Expect to get the entry with value 1.0
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_OK);
    ASSERT_EQ(iterator->current->docId, 1);
    ASSERT_EQ(iterator->lastDocId, 1);
    ASSERT_EQ(iterator->current->data.num.value, 1.0);

    // Read the next entry. Expect EOF since we have only one unique docId
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_EOF);
}

TEST_F(IndexIteratorTestEdges, GetCorrectValue) {
    // Add entries with the same ID but different values
    AddEntry(1, 1.0);
    AddEntry(1, 2.0);
    AddEntry(1, 3.0);
    // Create an iterator that reads only entries with value 2.0
    CreateIterator(2.0, 3.0);
    // Read the first entry. Expect to get the entry with value 2.0
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_OK);
    ASSERT_EQ(iterator->current->docId, 1);
    ASSERT_EQ(iterator->lastDocId, 1);
    ASSERT_EQ(iterator->current->data.num.value, 2.0);
    // Read the next entry. Expect EOF since we have only one unique docId with value 2.0
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_EOF);
}

TEST_F(IndexIteratorTestEdges, EOFAfterFiltering) {
    ASSERT_TRUE(InvertedIndex_GetDecoder(idx->flags).seeker == nullptr);
    // Fill the index with entries, all with value 1.0
    AddEntries(1, 1234, 1.0);
    // Create an iterator that reads only entries with value 2.0
    CreateIterator(2.0);
    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    ASSERT_EQ(iterator->SkipTo(iterator, 1), ITERATOR_EOF);
}

class IndexIteratorTestWithSeeker : public ::testing::Test {};
TEST_F(IndexIteratorTestWithSeeker, EOFAfterFiltering) {
    size_t memsize;
    InvertedIndex *idx = NewInvertedIndex(static_cast<IndexFlags>(INDEX_DEFAULT_FLAGS), 1, &memsize);
    ASSERT_TRUE(idx != nullptr);
    ASSERT_TRUE(InvertedIndex_GetDecoder(idx->flags).seeker != nullptr);
    auto encoder = InvertedIndex_GetEncoder(idx->flags);
    for (t_docId i = 1; i < 1000; ++i) {
      auto res = (RSIndexResult) {
        .docId = i,
        .fieldMask = 1,
        .freq = 1,
        .type = RSResultType::RSResultType_Term,
      };
      InvertedIndex_WriteEntryGeneric(idx, encoder, i, &res);
    }
    // Create an iterator that reads only entries with field mask 2
    QueryIterator *iterator = NewInvIndIterator_TermQuery(idx, nullptr, {.isFieldMask = true, .value = {.mask = 2}}, nullptr, 1.0);

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    ASSERT_EQ(iterator->SkipTo(iterator, 1), ITERATOR_EOF);

    // Cleanup
    iterator->Free(iterator);
    InvertedIndex_Free(idx);
}
