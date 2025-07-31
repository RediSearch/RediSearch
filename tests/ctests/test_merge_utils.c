/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid/merge_utils.h"
#include "rmutil/alloc.h"
#include "test_util.h"
#include "redisearch.h"
#include "rlookup.h"
#include "value.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>


/**
 * Helper function to create a test SearchResult with specified flags
 */
static SearchResult* createTestSearchResult(uint8_t flags) {
  SearchResult* result = rm_calloc(1, sizeof(SearchResult));
  if (!result) return NULL;

  result->docId = 1;  // Use a dummy docId
  result->score = 1.0;  // Use a dummy score
  result->flags = flags;
  result->scoreExplain = NULL;
  result->dmd = NULL;
  result->indexResult = NULL;
  memset(&result->rowdata, 0, sizeof(RLookupRow));

  return result;
}

/**
 * Test MergeFlags function with no flags set
 */
int testMergeFlags_NoFlags() {
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults with no flags
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Store results
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Test merging
  uint8_t mergedFlags = MergeFlags(hybridResult);
  ASSERT_EQUAL(mergedFlags, 0);

  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test MergeFlags function with Result_ExpiredDoc flag
 */
int testMergeFlags_ExpiredDoc() {
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults: one with expired flag, one without
  SearchResult* result1 = createTestSearchResult(0);  // No flags
  SearchResult* result2 = createTestSearchResult(Result_ExpiredDoc);  // Expired
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Store results
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Test merging - should have expired flag since ANY upstream has it
  uint8_t mergedFlags = MergeFlags(hybridResult);
  ASSERT(mergedFlags & Result_ExpiredDoc);

  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test MergeFlags function with NULL input
 */
int testMergeFlags_NullInput() {
  uint8_t mergedFlags = MergeFlags(NULL);
  ASSERT_EQUAL(mergedFlags, 0);
  return 0;
}

/**
 * Test MergeFlags function with single upstream
 */
int testMergeFlags_SingleUpstream() {
  HybridSearchResult* hybridResult = HybridSearchResult_New(1);
  ASSERT(hybridResult != NULL);

  // Create SearchResult with expired flag
  SearchResult* result1 = createTestSearchResult(Result_ExpiredDoc);
  ASSERT(result1 != NULL);

  // Store result
  hybridResult->searchResults[0] = result1;
  hybridResult->hasResults[0] = true;

  // Test merging
  uint8_t mergedFlags = MergeFlags(hybridResult);
  ASSERT(mergedFlags & Result_ExpiredDoc);

  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test UnionRLookupRows function with simple field union
 */
int testUnionRLookupRows_SimpleUnion() {
  // Create RLookup structure
  RLookup lookup = {0};
  RLookup_Init(&lookup, NULL);

  // Create keys
  RLookupKey *titleKey = RLookup_GetKey_Write(&lookup, "title", RLOOKUP_F_NOFLAGS);
  RLookupKey *contentKey = RLookup_GetKey_Write(&lookup, "content", RLOOKUP_F_NOFLAGS);
  ASSERT(titleKey != NULL);
  ASSERT(contentKey != NULL);

  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults with different fields
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add fields to upstream 1: title field
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, RS_StringVal(rm_strdup("Hello"), 5));

  // Add fields to upstream 2: content field
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, RS_StringVal(rm_strdup("World"), 5));

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Test in-place union: merge into result1's row
  UnionRLookupRows(hybridResult, &result1->rowdata, &lookup);

  // Verify both fields exist in result1's row (now merged)
  RSValue *titleValue = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *contentValue = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(titleValue != NULL);
  ASSERT(contentValue != NULL);
  ASSERT(RSValue_IsString(titleValue));
  ASSERT(RSValue_IsString(contentValue));
  RLookup_Cleanup(&lookup);
  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test UnionRLookupRows with in-place merging (dst = src1)
 */
int testUnionRLookupRows_InPlaceMerge() {
  // Create RLookup structure
  RLookup lookup = {0};
  RLookup_Init(&lookup, NULL);

  // Create keys
  RLookupKey *titleKey = RLookup_GetKey_Write(&lookup, "title", RLOOKUP_F_NOFLAGS);
  RLookupKey *contentKey = RLookup_GetKey_Write(&lookup, "content", RLOOKUP_F_NOFLAGS);
  ASSERT(titleKey != NULL);
  ASSERT(contentKey != NULL);

  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add fields to upstream 1: title field
  RSValue *titleVal = RS_StringVal(rm_strdup("Hello"), 5);
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal);

  // Add fields to upstream 2: content field
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, RS_StringVal(rm_strdup("World"), 5));

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Check initial refcount
  ASSERT_EQUAL(titleVal->refcount, 1);

  // Test in-place union: mergedRow = result1->rowdata (same memory!)
  UnionRLookupRows(hybridResult, &result1->rowdata, &lookup);

  // Check refcount after in-place merge - should still be 1, not 2!
  ASSERT_EQUAL(titleVal->refcount, 1);

  // Verify both fields exist
  RSValue *titleValue = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *contentValue = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(titleValue != NULL);
  ASSERT(contentValue != NULL);
  ASSERT(titleValue == titleVal);  // Should be same object

  // Cleanup
  RLookup_Cleanup(&lookup);
  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test UnionRLookupRows with overlapping fields (same key, same value)
 */
int testUnionRLookupRows_OverlappingFields() {
  // Create RLookup structure
  RLookup lookup = {0};
  RLookup_Init(&lookup, NULL);

  // Create keys
  RLookupKey *titleKey = RLookup_GetKey_Write(&lookup, "title", RLOOKUP_F_NOFLAGS);
  RLookupKey *contentKey = RLookup_GetKey_Write(&lookup, "content", RLOOKUP_F_NOFLAGS);
  ASSERT(titleKey != NULL);
  ASSERT(contentKey != NULL);

  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add SAME title field to BOTH upstreams (overlapping field)
  RSValue *titleVal1 = RS_StringVal(rm_strdup("Hello"), 5);
  RSValue *titleVal2 = RS_StringVal(rm_strdup("Hello"), 5);  // Same content, different object
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal1);
  RLookup_WriteOwnKey(titleKey, &result2->rowdata, titleVal2);

  // Add unique content field to upstream 2
  RSValue *contentVal = RS_StringVal(rm_strdup("World"), 5);
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, contentVal);

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Check initial refcounts
  ASSERT_EQUAL(titleVal1->refcount, 1);  // In result1
  ASSERT_EQUAL(titleVal2->refcount, 1);  // In result2
  ASSERT_EQUAL(contentVal->refcount, 1); // In result2

  // Test in-place union: merge into result1's row
  UnionRLookupRows(hybridResult, &result1->rowdata, &lookup);

  // Verify fields exist in result1's row (now merged)
  RSValue *mergedTitle = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *mergedContent = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(mergedTitle != NULL);
  ASSERT(mergedContent != NULL);

  // First upstream should win for overlapping field
  ASSERT(mergedTitle == titleVal1);  // Should be the first upstream's value
  ASSERT(mergedContent == contentVal);

  // Check refcounts after in-place merge
  ASSERT_EQUAL(titleVal1->refcount, 1);  // Only in result1 (no extra copy)
  ASSERT_EQUAL(titleVal2->refcount, 1);  // Only in result2 (not used in merge)
  ASSERT_EQUAL(contentVal->refcount, 2); // result2 + result1 (merged)
  RLookup_Cleanup(&lookup);
  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test idempotency: Union(A,B) then Union(A,B) again should be no-op
 */
int testUnionRLookupRows_Idempotency() {
  // Create RLookup structure
  RLookup lookup = {0};
  RLookup_Init(&lookup, NULL);

  // Create keys
  RLookupKey *titleKey = RLookup_GetKey_Write(&lookup, "title", RLOOKUP_F_NOFLAGS);
  RLookupKey *contentKey = RLookup_GetKey_Write(&lookup, "content", RLOOKUP_F_NOFLAGS);

  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);

  // Add fields: r1={title}, r2={content}
  RSValue *titleVal = RS_StringVal(rm_strdup("Hello"), 5);
  RSValue *contentVal = RS_StringVal(rm_strdup("World"), 5);
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal);
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, contentVal);

  // Store results
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Check initial refcounts
  ASSERT_EQUAL(titleVal->refcount, 1);   // Only in result1
  ASSERT_EQUAL(contentVal->refcount, 1); // Only in result2

  // First union: merge result2 into result1 (in-place)
  UnionRLookupRows(hybridResult, &result1->rowdata, &lookup);

  // After first union: result1 = {title, content}
  ASSERT_EQUAL(titleVal->refcount, 1);   // Still only in result1
  ASSERT_EQUAL(contentVal->refcount, 2); // result2 + result1

  // Second union: same operation should be idempotent (no-op)
  UnionRLookupRows(hybridResult, &result1->rowdata, &lookup);

  // After second union: refcounts should be UNCHANGED
  ASSERT_EQUAL(titleVal->refcount, 1);   // Still only in result1
  ASSERT_EQUAL(contentVal->refcount, 2); // Still result2 + result1 (no extra increment!)

  // Verify fields still exist
  RSValue *mergedTitle = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *mergedContent = RLookup_GetItem(contentKey, &result1->rowdata);
  ASSERT(mergedTitle == titleVal);
  ASSERT(mergedContent == contentVal);

  // Cleanup
  RLookup_Cleanup(&lookup);
  HybridSearchResult_Free(hybridResult);
  return 0;
}



TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testMergeFlags_NoFlags);
  TESTFUNC(testMergeFlags_ExpiredDoc);
  TESTFUNC(testMergeFlags_NullInput);
  TESTFUNC(testMergeFlags_SingleUpstream);
  TESTFUNC(testUnionRLookupRows_SimpleUnion);
  TESTFUNC(testUnionRLookupRows_InPlaceMerge);
  TESTFUNC(testUnionRLookupRows_OverlappingFields);
  TESTFUNC(testUnionRLookupRows_Idempotency);

})
