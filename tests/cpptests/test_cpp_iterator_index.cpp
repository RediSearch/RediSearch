/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

extern "C" {
#include "src/util/dict.h"
}

#include "gtest/gtest.h"
#include <vector>
#include "index_utils.h"

#include "src/iterators/inverted_index_iterator.h"
#include "src/index_result.h"
#include "src/tag_index.h"
#include "redisearch_rs/headers/triemap.h"
#include "redisearch_rs/headers/iterators_rs.h"

extern "C" {
#include "src/redis_index.h"
}

typedef enum RevalidateIndexType {
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
    InvertedIndex *missingIdx;
    TagIndex *tagIdx;
    InvertedIndex *tagInvIdx;

    // Query terms for Query-type iterators
    RSQueryTerm *tagQueryTerm;

    void SetUp() override {
        // Initialize Redis context
        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Initialize pointers
        spec = nullptr;
        sctx = nullptr;
        iterator = nullptr;
        missingIdx = nullptr;
        tagIdx = nullptr;
        tagInvIdx = nullptr;
        tagQueryTerm = nullptr;

        // Generate a set of document IDs for testing
        for (size_t i = 0; i < n_docs; ++i) {
            resultSet[i] = i + 1; // Document IDs start from 1
        }

        // Create the appropriate index based on the parameter
        switch (GetParam()) {
            case REVALIDATE_INDEX_TYPE_TAG_QUERY:
                SetupTagIndex();
                break;
            case REVALIDATE_INDEX_TYPE_MISSING_QUERY:
                SetupMissingIndex();
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

        // Clear query term pointer (it was freed by the iterator)
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
        missingIdx = nullptr;
        tagIdx = nullptr;
        tagInvIdx = nullptr;

        RedisModule_FreeThreadSafeContext(ctx);
    }

private:
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
        missingIdx = NewInvertedIndex(Index_DocIdsOnly, &memsize);

        // Populate with some data (for missing iterator, the content represents what's missing)
        for (size_t i = 0; i < n_docs; ++i) {
            RSIndexResult rec = {.docId = resultSet[i], .data = {.tag = RSResultData_Virtual}};
            InvertedIndex_WriteEntryGeneric(missingIdx, &rec);
        }

        // For test purposes, we'll add the missing index to the missingFieldDict
        // using direct dict manipulation through the dict's internals
        // First check if the dict is empty to avoid conflicts
        RS_ASSERT(spec->missingFieldDict != nullptr);

        // Use dictAdd which should be available, and check its return value
        int rc = dictAdd(spec->missingFieldDict, (void*)fs->fieldName, missingIdx);
        ASSERT_EQ(rc, DICT_OK) << "dictAdd failed: key already exists or other error";

        // Create missing iterator
        iterator = NewInvIndIterator_MissingQuery(missingIdx, sctx, fs->index);
    }

public:
    bool IsTagIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_TAG_QUERY;
    }

    bool IsMissingIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_MISSING_QUERY;
    }

    bool IsQueryIterator() const {
        return GetParam() == REVALIDATE_INDEX_TYPE_TAG_QUERY ||
               GetParam() == REVALIDATE_INDEX_TYPE_MISSING_QUERY;
    }
};

INSTANTIATE_TEST_SUITE_P(InvIndIteratorRevalidate, InvIndIteratorRevalidateTest, ::testing::Values(
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

        if (IsTagIterator() || IsMissingIterator()) {
            // For tag and missing iterators, we can simulate index disappearance by
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
