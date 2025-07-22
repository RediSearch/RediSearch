/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "gtest/gtest.h"
#include "iterator_util.h"
#include "index_utils.h"

#include "src/forward_index.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/tag_index.h"

// If I add extern "C" to the original one, I get issues compiling Rust
void FieldSpec_Cleanup(FieldSpec* fs) {
  // if `AS` was not used, name and path are pointing at the same string
  if (fs->fieldPath && fs->fieldName != fs->fieldPath) {
    HiddenString_Free(fs->fieldPath, true);
  }
  fs->fieldPath = NULL;
  if (fs->fieldName) {
    HiddenString_Free(fs->fieldName, true);
    fs->fieldName = NULL;
  }

  if (fs->types & INDEXFLD_T_VECTOR) {
    VecSimParams_Cleanup(&fs->vectorOpts.vecSimParams);
  }
}

typedef enum IndexIteratorType {
    TYPE_TERM_FULL,
    TYPE_NUMERIC_FULL,
    TYPE_TERM,
    TYPE_NUMERIC,
    TYPE_GENERIC,
} IndexIteratorType;

class IndexIteratorTest : public ::testing::TestWithParam<std::tuple<IndexIteratorType, bool>> {
protected:
    static constexpr size_t n_docs = 2.45 * std::max(INDEX_BLOCK_SIZE, INDEX_BLOCK_SIZE_DOCID_ONLY);
    std::array<t_docId, n_docs> resultSet;
    InvertedIndex *idx;
    QueryIterator *it_base;
    MockQueryEvalCtx q_mock;

    void SetUp() override {
        // Initialize TagIndex to nullptr
        tagIdx = nullptr;
        fs = nullptr;
        flt = nullptr;

        // Generate a set of document IDs for testing
        for (size_t i = 0; i < n_docs; ++i) {
            resultSet[i] = 2 * i + 1; // Document IDs start from 1
        }

        auto [indexIteratorType, withExpiration] = GetParam();

        if (withExpiration) {
            // Initialize the TTL table with some expiration data. Results should not be expired so the test passes as expected.
            for (size_t i = 0; i < n_docs; ++i) {
                q_mock.TTL_Add(resultSet[i]);
            }
        }

        switch (indexIteratorType) {
            case TYPE_TERM_FULL:
                SetTermsInvIndex();
                it_base = NewInvIndIterator_TermFull(idx);
                break;
            case TYPE_NUMERIC_FULL:
                SetNumericInvIndex();
                it_base = NewInvIndIterator_NumericFull(idx);
                break;
            case TYPE_TERM:
                SetTermsInvIndex();
                it_base = NewInvIndIterator_TermQuery(idx, &q_mock.sctx, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
                break;
            case TYPE_NUMERIC: {
                SetNumericInvIndex();
                ASSERT_TRUE(idx != nullptr);
                FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
                FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
                it_base = NewInvIndIterator_NumericQuery(idx, &q_mock.sctx, &fieldCtx, nullptr, -INFINITY, INFINITY);
            }
                break;
            case TYPE_GENERIC:
                SetGenericInvIndex();
                it_base = NewInvIndIterator_GenericQuery(idx, &q_mock.sctx, 0, FIELD_EXPIRATION_DEFAULT, 1.0);
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


INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTest, ::testing::Combine(
    ::testing::Values(
        TYPE_TERM_FULL,
        TYPE_NUMERIC_FULL,
        TYPE_TERM,
        TYPE_NUMERIC,
        TYPE_GENERIC
    ),
    ::testing::Bool()
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
    std::unique_ptr<MockQueryEvalCtx> mockEvalCtx;
    FieldSpec *fs;

    void SetUp() override {
        size_t memsize;
        idx = NewInvertedIndex(Index_StoreNumeric, 1, &memsize);
        ASSERT_TRUE(idx != nullptr);
        iterator = nullptr;
        flt = nullptr;
        fs = nullptr;
    }

    void TearDown() override {
        if (flt) NumericFilter_Free(flt);
        if (iterator) iterator->Free(iterator);
        InvertedIndex_Free(idx);
        if (fs) {
          FieldSpec_Cleanup(fs);
          rm_free(fs);
        }
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

        // Create a field spec for the numeric filter
        fs = (FieldSpec*)rm_calloc(1, sizeof(FieldSpec));
        fs->fieldName = NewHiddenString("dummy_field", strlen("dummy_field"), true);
        fs->fieldPath = fs->fieldName; // Use same string for path
        fs->types = INDEXFLD_T_NUMERIC;
        fs->index = 0;
        fs->ftId = 1;

        // Create a numeric filter with the field spec
        flt = NewNumericFilter(min, max, 1, 1, 1, fs);

        // Create a mock RedisSearchCtx with properly initialized spec
        mockEvalCtx = std::make_unique<MockQueryEvalCtx>(1234, 1234);

        // Make sure the spec name is initialized in the mock context
        if (!mockEvalCtx->qctx.sctx->spec->specName) {
            mockEvalCtx->qctx.sctx->spec->specName = NewHiddenString("dummy_index", strlen("dummy_index"), true);
        }

        // Initialize keysDict if it doesn't exist
        if (!mockEvalCtx->qctx.sctx->spec->keysDict) {
            // Initialize the keysDict in the spec
            IndexSpec_MakeKeyless(mockEvalCtx->qctx.sctx->spec);
        }

        iterator = NewInvIndIterator_NumericQuery(idx, mockEvalCtx->qctx.sctx, &fieldCtx, flt, min, max);
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

class IndexIteratorTestExpiration : public ::testing::TestWithParam<IndexFlags> {
protected:
    static constexpr size_t n_docs = std::max(INDEX_BLOCK_SIZE, INDEX_BLOCK_SIZE_DOCID_ONLY);
    InvertedIndex *idx;
    QueryIterator *it_base;
    MockQueryEvalCtx q_mock;

    void SetUp() override {
        IndexFlags flags = GetParam();

        size_t dummy;
        idx = NewInvertedIndex(flags, 1, &dummy);

        t_fieldIndex fieldIndex = 0b101010; // 42
        t_fieldMask fieldMask = fieldIndex;
        if (flags & Index_WideSchema) {
            fieldMask |= fieldMask << 64; // Wide field mask for wide schema
        }

        // Add docs to the index
        IndexEncoder encoder = InvertedIndex_GetEncoder(flags);
        RSIndexResult res = {
            .fieldMask = fieldMask,
        };
        for (size_t i = 1; i <= n_docs; ++i) {
            res.docId = i;
            InvertedIndex_WriteEntryGeneric(idx, encoder, i, &res);
            InvertedIndex_WriteEntryGeneric(idx, encoder, i, &res); // Second write will fail if multi-value is not supported
        }

        // Make every even document ID field expired
        for (size_t i = 2; i <= n_docs; i += 2) {
            if (flags & Index_StoreNumeric || flags == Index_DocIdsOnly) {
                q_mock.TTL_Add(i, fieldIndex, {1, 1}); // Already expired
            } else {
                q_mock.TTL_Add(i, fieldMask, {1, 1}); // Already expired
            }
        }
        // Set up the mock current time
        q_mock.sctx.time.current = {100, 100};

        // Create the iterator based on the flags
        if (flags & Index_StoreNumeric) {
            FieldFilterContext fieldCtx = {.field = {false, fieldIndex}, .predicate = FIELD_EXPIRATION_DEFAULT};
            it_base = NewInvIndIterator_NumericQuery(idx, &q_mock.sctx, &fieldCtx, nullptr, -INFINITY, INFINITY);
        } else if (flags == Index_DocIdsOnly) {
            it_base = NewInvIndIterator_GenericQuery(idx, &q_mock.sctx, fieldIndex, FIELD_EXPIRATION_DEFAULT, 1.0);
        } else {
            it_base = NewInvIndIterator_TermQuery(idx, &q_mock.sctx, {true, fieldMask}, nullptr, 1.0);
        }
    }

    void TearDown() override {
        it_base->Free(it_base);
        InvertedIndex_Free(idx);
    }
};
INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTestExpiration, ::testing::Values(
    Index_DocIdsOnly,                                                                       // Single field
    Index_StoreNumeric,                                                                     // Single field, multi-value
    Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets,                      // field-mask, with seeker
    Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema    // wide field-mask
));

TEST_P(IndexIteratorTestExpiration, Read) {
    InvIndIterator *it = (InvIndIterator *)it_base;
    IteratorStatus rc;

    // Test reading until EOF - expect only odd document IDs
    size_t i = 0;
    while ((rc = it_base->Read(it_base)) == ITERATOR_OK) {
        ASSERT_EQ(it->base.current->docId, 2*i + 1);
        ASSERT_EQ(it->base.lastDocId, 2*i + 1);
        ASSERT_FALSE(it->base.atEOF);
        i++;
    }
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_TRUE(it->base.atEOF);
    ASSERT_EQ(it_base->Read(it_base), ITERATOR_EOF); // Reading after EOF should return EOF
    ASSERT_EQ(i, n_docs / 2 + (n_docs % 2)) << "Expected to read half of the documents (odd IDs only)";
}

TEST_P(IndexIteratorTestExpiration, SkipTo) {
    InvIndIterator *it = (InvIndIterator *)it_base;
    IteratorStatus rc;

    // Test skipping to various document IDs
    it_base->Rewind(it_base);

    // Skip to odd IDs should work
    for (t_docId id = 1; id <= n_docs; id += 2) {
        rc = it_base->SkipTo(it_base, id);
        ASSERT_EQ(rc, ITERATOR_OK);
        ASSERT_EQ(it->base.current->docId, id);
        ASSERT_EQ(it->base.lastDocId, id);
    }

    // Test skipping to even IDs - should skip to next odd ID
    it_base->Rewind(it_base);
    for (t_docId id = 2; id <= n_docs; id += 2) {
        rc = it_base->SkipTo(it_base, id);
        if (id + 1 <= n_docs) {
            ASSERT_EQ(rc, ITERATOR_NOTFOUND);
            ASSERT_EQ(it->base.current->docId, id + 1);
            ASSERT_EQ(it->base.lastDocId, id + 1);
        } else {
            ASSERT_EQ(rc, ITERATOR_EOF);
            ASSERT_TRUE(it->base.atEOF);
        }
    }

    // Test skipping to ID beyond range
    it_base->Rewind(it_base);
    rc = it_base->SkipTo(it_base, n_docs + 1);
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_TRUE(it->base.atEOF);
}
