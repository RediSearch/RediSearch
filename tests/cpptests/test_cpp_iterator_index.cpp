/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

extern "C" {
#include "src/util/dict.h"
}

#include "gtest/gtest.h"
#include "iterator_util.h"
#include <vector>
#include "index_utils.h"

#include "src/forward_index.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/index_result.h"
#include "src/tag_index.h"
#include "redisearch_rs/headers/triemap.h"
#include "redisearch_rs/headers/iterators_rs.h"

extern "C" {
#include "src/redis_index.h"
}

class IndexIteratorTest : public ::testing::TestWithParam<bool> {
protected:
    static constexpr size_t n_docs = 2.45 * 1000;
    std::array<t_docId, n_docs> resultSet;
    InvertedIndex *idx;
    QueryIterator *it_base;
    MockQueryEvalCtx q_mock;

    void SetUp() override {
        // Generate a set of document IDs for testing
        for (size_t i = 0; i < n_docs; ++i) {
            resultSet[i] = 2 * i + 1; // Document IDs start from 1
        }

        bool withExpiration = GetParam();

        if (withExpiration) {
            // Initialize the TTL table with some expiration data. Results should not be expired so the test passes as expected.
            for (size_t i = 0; i < n_docs; ++i) {
                q_mock.TTL_Add(resultSet[i]);
            }
        }

        SetTermsInvIndex();
        RSToken tok = {.str = const_cast<char*>("term"), .len = 4, .flags = 0};
        it_base = NewInvIndIterator_TermQuery(idx, &q_mock.sctx, {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL}, NewQueryTerm(&tok, 1), 1.0);
    }
    void TearDown() override {
        it_base->Free(it_base);
        InvertedIndex_Free(idx);
    }

private:
    void SetTermsInvIndex() {
        // This function should populate the InvertedIndex with terms
        size_t memsize;
        idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), &memsize);
        for (size_t i = 0; i < n_docs; ++i) {
            ForwardIndexEntry h = {0};
            h.docId = resultSet[i];
            h.fieldMask = i + 1;
            h.freq = i + 1;
            h.term = "term";
            h.len = 4; // Length of the term "term"

            h.vw = NewVarintVectorWriter(8);
            VVW_Write(h.vw, i); // Just writing the index as a value
            InvertedIndex_WriteForwardIndexEntry(idx, &h);
            VVW_Free(h.vw);
        }
    }
};

INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTest, ::testing::Bool());


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
    ASSERT_EQ(it_base->NumEstimated(it_base), InvertedIndex_NumDocs(idx));
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

class IndexIteratorTestExpiration : public ::testing::TestWithParam<IndexFlags> {
  protected:
      static constexpr size_t n_docs = 1000;
      InvertedIndex *idx;
      QueryIterator *it_base;
      MockQueryEvalCtx q_mock;

      void SetUp() override {
          IndexFlags flags = GetParam();

          size_t dummy;
          idx = NewInvertedIndex(flags, &dummy);

          t_fieldIndex fieldIndex = 0b101010; // 42
          t_fieldMask fieldMask = fieldIndex;
          if (flags & Index_WideSchema) {
              fieldMask |= fieldMask << 64; // Wide field mask for wide schema
          }

          // Add docs to the index
          RSIndexResult res = {
              .fieldMask = fieldMask,
              .data = {.term_tag = RSResultData_Term},
          };

          for (size_t i = 1; i <= n_docs; ++i) {
              res.docId = i;
              InvertedIndex_WriteEntryGeneric(idx, &res);
              InvertedIndex_WriteEntryGeneric(idx, &res); // Second write will fail if multi-value is not supported
          }

          // Make every even document ID field expired
          for (size_t i = 2; i <= n_docs; i += 2) {
              q_mock.TTL_Add(i, fieldMask, {1, 1}); // Already expired
          }
          // Set up the mock current time
          q_mock.sctx.time.current = {100, 100};

          // Create the iterator based on the flags
          RSToken tok = {.str = const_cast<char*>("term"), .len = 4, .flags = 0};
          it_base = NewInvIndIterator_TermQuery(idx, &q_mock.sctx, {.mask_tag = FieldMaskOrIndex_Mask, .mask = fieldMask}, NewQueryTerm(&tok, 1), 1.0);
      }

      void TearDown() override {
          it_base->Free(it_base);
          InvertedIndex_Free(idx);
      }
  };

  INSTANTIATE_TEST_SUITE_P(IndexIterator, IndexIteratorTestExpiration, ::testing::Values(
      Index_DocIdsOnly,                                                                       // Single field
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

typedef enum RevalidateIndexType {
    REVALIDATE_INDEX_TYPE_TERM_QUERY,
    REVALIDATE_INDEX_TYPE_TAG_QUERY,
    REVALIDATE_INDEX_TYPE_MISSING_QUERY,
} RevalidateIndexType;

/**
 * Test class for testing the Revalidate feature of InvIndIterator with different index types.
 *
 * This test class creates indices for TERM and TAG field types and tests the
 * Revalidate functionality of their corresponding iterators. The Revalidate feature is
 * used to check if an iterator's underlying index is still valid (e.g., hasn't been
 * garbage collected or modified).
 *
 * Current implementation status:
 * - TERM iterators: Basic functionality works, but Revalidate tests require proper
 *   RedisSearchCtx setup for TermCheckAbort to work correctly
 * - TAG iterators: Basic functionality works, but Revalidate tests require proper
 *   RedisSearchCtx setup for TagCheckAbort to work correctly
 *
 * For complete TERM and TAG Revalidate testing, the following would be needed:
 * 1. Proper RedisSearchCtx initialization with the created IndexSpec
 * 2. Integration with the Redis key-value store for index lookup
 * 3. Proper cleanup of Redis state between tests
 *
 * The test framework demonstrates how to:
 * - Create different types of indices using IndexSpec_ParseC
 * - Populate indices with test data
 * - Create appropriate iterators for each index type
 * - Test basic iterator functionality (Read, Rewind, SkipTo)
 * - Test Revalidate functionality where possible
 */
class InvIndIteratorRevalidateTest : public ::testing::TestWithParam<RevalidateIndexType> {
protected:
    static constexpr size_t n_docs = 10;
    std::array<t_docId, n_docs> resultSet;
    IndexSpec *spec;
    RedisModuleCtx *ctx;
    RedisSearchCtx *sctx;
    QueryIterator *iterator;

    // For different index types
    InvertedIndex *termIdx;
    TagIndex *tagIdx;
    InvertedIndex *tagInvIdx;

    // Query terms for Query-type iterators
    RSQueryTerm *queryTerm;
    RSQueryTerm *tagQueryTerm;

    void SetUp() override {
        // Initialize Redis context
        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Initialize pointers
        spec = nullptr;
        sctx = nullptr;
        iterator = nullptr;
        termIdx = nullptr;
        tagIdx = nullptr;
        tagInvIdx = nullptr;
        queryTerm = nullptr;
        tagQueryTerm = nullptr;

        // Generate a set of document IDs for testing
        for (size_t i = 0; i < n_docs; ++i) {
            resultSet[i] = i + 1; // Document IDs start from 1
        }

        // Create the appropriate index based on the parameter
        switch (GetParam()) {
            case REVALIDATE_INDEX_TYPE_TERM_QUERY:
                SetupTermIndex();
                break;
            case REVALIDATE_INDEX_TYPE_TAG_QUERY:
                SetupTagIndex();
                break;
            case REVALIDATE_INDEX_TYPE_MISSING_QUERY:
                SetupMissingIndex();      // Missing query version
                break;
            default:
                FAIL() << "Unknown index type: " << GetParam();
                break;
        }
    }

    void TearDown() override {
        // Free iterator first (this will free the IndexResult and associated terms)
        if (iterator) {
            iterator->Free(iterator);
            iterator = nullptr;
        }

        // Clear query term pointers (they were freed by the iterator)
        queryTerm = nullptr;
        tagQueryTerm = nullptr;

        // Free search context
        if (sctx) {
            SearchCtx_Free(sctx);
            sctx = nullptr;
        }

        // Note: tagIdx is now owned by the IndexSpec's keysDict and will be freed
        // automatically when the spec is removed from globals

        // Remove spec from globals (this may free associated indices)
        if (spec) {
            IndexSpec_RemoveFromGlobals(spec->own_ref, false);
            spec = nullptr;
        }

        // Clear pointers (don't free directly as they may have been freed by iterator or spec)
        termIdx = nullptr;
        tagIdx = nullptr;
        tagInvIdx = nullptr;

        RedisModule_FreeThreadSafeContext(ctx);
    }

private:
    void SetupTermIndex() {
        // Create IndexSpec for TEXT field
        const char *args[] = {"SCHEMA", "text_field", "TEXT"};
        QueryError err = QueryError_Default();
        StrongRef ref = IndexSpec_ParseC("term_idx", args, sizeof(args) / sizeof(const char *), &err);
        spec = (IndexSpec *)StrongRef_Get(ref);
        ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
        ASSERT_TRUE(spec);

        // Add the spec to the global dictionary so it can be found by name
        Spec_AddToDict(spec->own_ref.rm);

        // Create RedisSearchCtx
        sctx = NewSearchCtxC(ctx, "term_idx", false);
        ASSERT_TRUE(sctx != nullptr);

        // Get the term inverted index from the spec using Redis_OpenInvertedIndex
        // This will create the index and add it to the spec's keysDict properly
        bool isNew;
        termIdx = Redis_OpenInvertedIndex(sctx, "term", strlen("term"), 1, &isNew);
        ASSERT_TRUE(termIdx != nullptr);

        // Populate with term data
        for (size_t i = 0; i < n_docs; ++i) {
            ForwardIndexEntry h = {0};
            h.docId = resultSet[i];
            h.fieldMask = i + 1;
            h.freq = i + 1;
            h.term = "term";
            h.len = 4; // Length of "term"

            h.vw = NewVarintVectorWriter(8);
            VVW_Write(h.vw, i); // Just writing the index as a value
            InvertedIndex_WriteForwardIndexEntry(termIdx, &h);
            VVW_Free(h.vw);
        }

        // Query version with proper context and term data
        RSToken tok = {.str = const_cast<char*>("term"), .len = 4, .flags = 0};
        queryTerm = NewQueryTerm(&tok, 1);
        FieldMaskOrIndex fieldMaskOrIndex = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
        iterator = NewInvIndIterator_TermQuery(termIdx, sctx, fieldMaskOrIndex, queryTerm, 1.0);
    }

    void SetupTagIndex() {
        // Create IndexSpec for TAG field
        const char *args[] = {"SCHEMA", "tag_field", "TAG"};
        QueryError err = QueryError_Default();
        StrongRef ref = IndexSpec_ParseC("tag_idx", args, sizeof(args) / sizeof(const char *), &err);
        spec = (IndexSpec *)StrongRef_Get(ref);
        ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
        ASSERT_TRUE(spec);

        // Add the spec to the global dictionary so it can be found by name
        Spec_AddToDict(spec->own_ref.rm);

        // Create RedisSearchCtx
        sctx = NewSearchCtxC(ctx, "tag_idx", false);
        ASSERT_TRUE(sctx != nullptr);

        // Get the tag index from the spec using TagIndex_Open
        const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, "tag_field", strlen("tag_field"));
        ASSERT_TRUE(fs != nullptr);
        tagIdx = TagIndex_Open(const_cast<FieldSpec *>(fs), CREATE_INDEX, NULL);
        ASSERT_TRUE(tagIdx != nullptr);

        // Create tag inverted index for a specific tag value
        size_t sz;
        tagInvIdx = TagIndex_OpenIndex(tagIdx, "test_tag", 8, CREATE_INDEX, &sz);

        // Populate with tag data using the simpler approach
        for (size_t i = 0; i < n_docs; ++i) {
            RSIndexResult rec = {.docId = resultSet[i], .data = {.tag = RSResultData_Virtual}};
            InvertedIndex_WriteEntryGeneric(tagInvIdx, &rec);
        }

        // Query version with proper context and term data
        RSToken tagTok = {.str = const_cast<char*>("test_tag"), .len = 8, .flags = 0};
        tagQueryTerm = NewQueryTerm(&tagTok, 1);
        FieldMaskOrIndex tagFieldMaskOrIndex = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
        iterator = NewInvIndIterator_TagQuery(tagInvIdx, tagIdx, sctx, tagFieldMaskOrIndex, tagQueryTerm, 1.0);
    }

    void SetupMissingIndex() {
        // Create IndexSpec for TEXT field (missing uses any field type)
        const char *args[] = {"SCHEMA", "text_field", "TEXT"};
        QueryError err = QueryError_Default();
        StrongRef ref = IndexSpec_ParseC("missing_idx", args, sizeof(args) / sizeof(const char *), &err);
        spec = (IndexSpec *)StrongRef_Get(ref);
        ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
        ASSERT_TRUE(spec);

        // Add the spec to the global dictionary so it can be found by name
        Spec_AddToDict(spec->own_ref.rm);

        // Create RedisSearchCtx
        sctx = NewSearchCtxC(ctx, "missing_idx", false);
        ASSERT_TRUE(sctx != nullptr);

        // Get the field spec for the missing iterator
        const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, "text_field", strlen("text_field"));
        ASSERT_TRUE(fs != nullptr);

        // Create a missing field index for the field
        size_t memsize;
        termIdx = NewInvertedIndex(Index_DocIdsOnly, &memsize);

        // Populate with some data (for missing iterator, the content represents what's missing)
        for (size_t i = 0; i < n_docs; ++i) {
            RSIndexResult rec = {.docId = resultSet[i], .data = {.tag = RSResultData_Virtual}};
            InvertedIndex_WriteEntryGeneric(termIdx, &rec);
        }

        // For test purposes, we'll add the missing index to the missingFieldDict
        // using direct dict manipulation through the dict's internals
        // First check if the dict is empty to avoid conflicts
        RS_ASSERT(spec->missingFieldDict != nullptr);

        // Use dictAdd which should be available, and check its return value
        int rc = dictAdd(spec->missingFieldDict, (void*)fs->fieldName, termIdx);
        ASSERT_EQ(rc, DICT_OK) << "dictAdd failed: key already exists or other error";

        // Create missing iterator
        iterator = NewInvIndIterator_MissingQuery(termIdx, sctx, fs->index);
    }

public:
    bool IsTermIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_TERM_QUERY;
    }

    bool IsTagIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_TAG_QUERY;
    }

    bool IsMissingIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_MISSING_QUERY;
    }

    bool IsQueryIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_TERM_QUERY ||
               GetParam() == REVALIDATE_INDEX_TYPE_TAG_QUERY ||
               GetParam() == REVALIDATE_INDEX_TYPE_MISSING_QUERY;
    }
};

INSTANTIATE_TEST_SUITE_P(InvIndIteratorRevalidate, InvIndIteratorRevalidateTest, ::testing::Values(
    REVALIDATE_INDEX_TYPE_TERM_QUERY,
    REVALIDATE_INDEX_TYPE_TAG_QUERY,
    REVALIDATE_INDEX_TYPE_MISSING_QUERY
));

// Basic test to ensure the iterator setup works correctly
TEST_P(InvIndIteratorRevalidateTest, BasicIteratorFunctionality) {
    ASSERT_TRUE(iterator != nullptr);

    // Test that we can read all documents
    size_t count = 0;
    IteratorStatus rc;
    while ((rc = iterator->Read(iterator)) == ITERATOR_OK) {
        ASSERT_EQ(iterator->current->docId, resultSet[count]);
        count++;
    }
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_EQ(count, n_docs);

    // Test rewind functionality
    iterator->Rewind(iterator);
    ASSERT_EQ(iterator->lastDocId, 0);
    ASSERT_FALSE(iterator->atEOF);
}

// Test basic Revalidate functionality - should return VALIDATE_OK when index is valid
TEST_P(InvIndIteratorRevalidateTest, RevalidateBasic) {
    ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_OK);
    ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);
}

// Test Revalidate when iterator is at EOF
TEST_P(InvIndIteratorRevalidateTest, RevalidateAtEOF) {
    // Read all documents to reach EOF
    IteratorStatus rc;
    while ((rc = iterator->Read(iterator)) == ITERATOR_OK) {
        // Continue reading
    }
    ASSERT_EQ(rc, ITERATOR_EOF);
    ASSERT_TRUE(iterator->atEOF);

    ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);
}

// Test Revalidate returns VALIDATE_ABORTED when the underlying index disappears
TEST_P(InvIndIteratorRevalidateTest, RevalidateAfterIndexDisappears) {
    // First, verify the iterator works normally and read at least one document
    // This is important because CheckAbort functions need current->data.term.term to be set
    ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);
    ASSERT_EQ(iterator->Read(iterator), ITERATOR_OK);
    ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);

    // Only Query iterators have sctx and can detect index disappearance
    if (IsQueryIterator()) {
        // For this test, we'll simulate index disappearance by directly manipulating
        // the iterator's stored index pointer to make it different from what would
        // be returned by the lookup functions. This simulates the case where the
        // index was garbage collected and recreated.

        if (IsTermIterator() || IsTagIterator() || IsMissingIterator()) {
            // For term and tag iterators, we can simulate index disappearance by
            // setting the iterator's idx pointer to a different value than what
            // the lookup would return. This simulates the GC scenario.
            InvIndIterator *invIt = (InvIndIterator *)iterator;

            // Create a dummy index to simulate the "new" index that would be returned
            // by the lookup after GC
            size_t memsize;
            InvertedIndex *dummyIdx = NewInvertedIndex(InvIndIterator_GetReaderFlags(invIt), &memsize);

            // Temporarily replace the iterator's index pointer
            InvIndIterator_Rs_SwapIndex(invIt, dummyIdx);

            // Now Revalidate should return VALIDATE_ABORTED because the stored index
            // doesn't match what the lookup returns
            ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_ABORTED);

            // Restore the original index pointer for proper cleanup
            InvIndIterator_Rs_SwapIndex(invIt, dummyIdx);

            // Clean up the dummy index
            InvertedIndex_Free(dummyIdx);
        } else {
            FAIL() << "RevalidateAfterIndexDisappears not implemented for this iterator type";
        }
    } else {
        // Full iterators don't have sctx, so they can't detect disappearance
        // They will always return VALIDATE_OK regardless of index state
        ASSERT_EQ(iterator->Revalidate(iterator), VALIDATE_OK);
    }
}
