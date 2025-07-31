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

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testMergeFlags_NoFlags);
  TESTFUNC(testMergeFlags_ExpiredDoc);
  TESTFUNC(testMergeFlags_NullInput);
  TESTFUNC(testMergeFlags_SingleUpstream);
})
