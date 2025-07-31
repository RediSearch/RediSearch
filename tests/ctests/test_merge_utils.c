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
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/**
 * Debug function to print RSScoreExplain structure recursively
 */
static void printScoreExplain(RSScoreExplain *scrExp, int depth) {
  if (!scrExp) {
    printf("%*sNULL\n", depth * 2, "");
    return;
  }

  printf("%*s[%d children] %s\n", depth * 2, "", scrExp->numChildren,
         scrExp->str ? scrExp->str : "(no string)");

  for (int i = 0; i < scrExp->numChildren; i++) {
    printf("%*sChild %d:\n", depth * 2, "", i);
    printScoreExplain(&scrExp->children[i], depth + 1);
  }
}

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
  uint8_t target_flags = 0;
  uint8_t source_flags = 0;

  // Test merging no flags
  MergeFlags(&target_flags, &source_flags);
  ASSERT_EQUAL(target_flags, 0);

  return 0;
}

/**
 * Test MergeFlags function with Result_ExpiredDoc flag
 */
int testMergeFlags_ExpiredDoc() {
  uint8_t target_flags = 0;  // No flags initially
  uint8_t source_flags = Result_ExpiredDoc;  // Source has expired flag

  // Test merging expired flag
  MergeFlags(&target_flags, &source_flags);
  ASSERT(target_flags & Result_ExpiredDoc);

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

  // Create SearchResults with different fields
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add fields to result1: title field
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, RS_StringVal(rm_strdup("Hello"), 5));

  // Add fields to result2: content field
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, RS_StringVal(rm_strdup("World"), 5));

  // Test union: merge result2's fields into result1's row
  UnionRLookupRows(&result1->rowdata, &result2->rowdata, &lookup);

  // Verify both fields exist in result1's row (now merged)
  RSValue *titleValue = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *contentValue = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(titleValue != NULL);
  ASSERT(contentValue != NULL);
  ASSERT(RSValue_IsString(titleValue));
  ASSERT(RSValue_IsString(contentValue));

  // Cleanup
  RLookup_Cleanup(&lookup);
  SearchResult_Destroy(result1);
  SearchResult_Destroy(result2);
  rm_free(result1);
  rm_free(result2);
  return 0;
}

/**
 * Test UnionRLookupRows with reference counting
 */
int testUnionRLookupRows_RefCounting() {
  // Create RLookup structure
  RLookup lookup = {0};
  RLookup_Init(&lookup, NULL);

  // Create keys
  RLookupKey *titleKey = RLookup_GetKey_Write(&lookup, "title", RLOOKUP_F_NOFLAGS);
  RLookupKey *contentKey = RLookup_GetKey_Write(&lookup, "content", RLOOKUP_F_NOFLAGS);
  ASSERT(titleKey != NULL);
  ASSERT(contentKey != NULL);

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add fields to result1: title field
  RSValue *titleVal = RS_StringVal(rm_strdup("Hello"), 5);
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal);

  // Add fields to result2: content field
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, RS_StringVal(rm_strdup("World"), 5));

  // Check initial refcount
  ASSERT_EQUAL(titleVal->refcount, 1);

  // Test union: merge result2's fields into result1's row
  UnionRLookupRows(&result1->rowdata, &result2->rowdata, &lookup);

  // Check refcount after merge - should still be 1 for existing field
  ASSERT_EQUAL(titleVal->refcount, 1);

  // Verify both fields exist
  RSValue *titleValue = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *contentValue = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(titleValue != NULL);
  ASSERT(contentValue != NULL);
  ASSERT(titleValue == titleVal);  // Should be same object

  // Cleanup
  RLookup_Cleanup(&lookup);
  SearchResult_Destroy(result1);
  SearchResult_Destroy(result2);
  rm_free(result1);
  rm_free(result2);
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

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Add SAME title field to BOTH results (overlapping field)
  RSValue *titleVal1 = RS_StringVal(rm_strdup("Hello"), 5);
  RSValue *titleVal2 = RS_StringVal(rm_strdup("Hello"), 5);  // Same content, different object
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal1);
  RLookup_WriteOwnKey(titleKey, &result2->rowdata, titleVal2);

  // Add unique content field to result2
  RSValue *contentVal = RS_StringVal(rm_strdup("World"), 5);
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, contentVal);

  // Check initial refcounts
  ASSERT_EQUAL(titleVal1->refcount, 1);  // In result1
  ASSERT_EQUAL(titleVal2->refcount, 1);  // In result2
  ASSERT_EQUAL(contentVal->refcount, 1); // In result2

  // Test union: merge result2's fields into result1's row
  UnionRLookupRows(&result1->rowdata, &result2->rowdata, &lookup);

  // Verify fields exist in result1's row (now merged)
  RSValue *mergedTitle = RLookup_GetItem(titleKey, &result1->rowdata);
  RSValue *mergedContent = RLookup_GetItem(contentKey, &result1->rowdata);

  ASSERT(mergedTitle != NULL);
  ASSERT(mergedContent != NULL);

  // Target should win for overlapping field (no change since it already exists)
  ASSERT(mergedTitle == titleVal1);  // Should be the target's value
  ASSERT(mergedContent == contentVal);

  // Check refcounts after merge
  ASSERT_EQUAL(titleVal1->refcount, 1);  // Only in result1 (no extra copy)
  ASSERT_EQUAL(titleVal2->refcount, 1);  // Only in result2 (not used in merge)
  ASSERT_EQUAL(contentVal->refcount, 2); // result2 + result1 (merged)

  // Cleanup
  RLookup_Cleanup(&lookup);
  SearchResult_Destroy(result1);
  SearchResult_Destroy(result2);
  rm_free(result1);
  rm_free(result2);
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

  // Create SearchResults
  SearchResult* result1 = createTestSearchResult(0);
  SearchResult* result2 = createTestSearchResult(0);

  // Add fields: r1={title}, r2={content}
  RSValue *titleVal = RS_StringVal(rm_strdup("Hello"), 5);
  RSValue *contentVal = RS_StringVal(rm_strdup("World"), 5);
  RLookup_WriteOwnKey(titleKey, &result1->rowdata, titleVal);
  RLookup_WriteOwnKey(contentKey, &result2->rowdata, contentVal);

  // Check initial refcounts
  ASSERT_EQUAL(titleVal->refcount, 1);   // Only in result1
  ASSERT_EQUAL(contentVal->refcount, 1); // Only in result2

  // First union: merge result2 into result1
  UnionRLookupRows(&result1->rowdata, &result2->rowdata, &lookup);

  // After first union: result1 = {title, content}
  ASSERT_EQUAL(titleVal->refcount, 1);   // Still only in result1
  ASSERT_EQUAL(contentVal->refcount, 2); // result2 + result1

  // Second union: same operation should be idempotent (no-op)
  UnionRLookupRows(&result1->rowdata, &result2->rowdata, &lookup);

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
  SearchResult_Destroy(result1);
  SearchResult_Destroy(result2);
  rm_free(result1);
  rm_free(result2);
  return 0;
}

/**
 * Test mergeHybridWrapper function with RRF scoring and targetIndex = 0 (first result as target)
 */
int testMergeHybridWrapper_RRF_TargetIndex0() {
  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults with mock explanations
  SearchResult* result1 = createTestSearchResult(1);
  SearchResult* result2 = createTestSearchResult(2);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Create mock score explanations for upstream results
  result1->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result1->scoreExplain->str = rm_strdup("Upstream1: TF-IDF score = 0.85");
  result1->scoreExplain->numChildren = 0;
  result1->scoreExplain->children = NULL;

  result2->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result2->scoreExplain->str = rm_strdup("Upstream2: Vector similarity = 0.92");
  result2->scoreExplain->numChildren = 0;
  result2->scoreExplain->children = NULL;

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Create RRF scoring context
  HybridScoringContext scoringCtx = {0};
  scoringCtx.scoringType = HYBRID_SCORING_RRF;
  scoringCtx.rrfCtx.k = 60;

  // Set up ranks for RRF calculation
  double ranks[2] = {1.0, 2.0}; // rank 1 and rank 2

  // Test: Call mergeHybridWrapper with result1 as target (index 0)
  double rrfScore = mergeHybridWrapper(hybridResult, 0, ranks, &scoringCtx);

  // Verify: RRF score was calculated and returned
  ASSERT(rrfScore > 0.0);
  double expectedRRF = (1.0 / (60 + 1)) + (1.0 / (60 + 2)); // 1/61 + 1/62
  ASSERT(fabs(rrfScore - expectedRRF) < 0.0001); // Allow small floating point difference

  // Verify: First SearchResult now has scoreExplain populated
  ASSERT(result1->scoreExplain != NULL);
  ASSERT(result1->scoreExplain->str != NULL);

  // Verify: Explanation string matches expected RRF formula exactly
  ASSERT(strcmp(result1->scoreExplain->str, "RRF: 0.03: 1/(60+1) + 1/(60+2)") == 0);

  // Verify: Structure matches observed output - [2 children] with proper hierarchy
  ASSERT(result1->scoreExplain->numChildren == 2);
  ASSERT(result1->scoreExplain->children != NULL);

  // Verify: Children have no sub-children (as observed: [0 children])
  ASSERT(result1->scoreExplain->children[0].numChildren == 0);
  ASSERT(result1->scoreExplain->children[1].numChildren == 0);

  // Verify: Children contain exact upstream explanations as seen in output
  ASSERT(result1->scoreExplain->children[0].str != NULL);
  ASSERT(result1->scoreExplain->children[1].str != NULL);

  // Test against actual results we observed
  ASSERT(strcmp(result1->scoreExplain->children[0].str, "Upstream1: TF-IDF score = 0.85") == 0);
  ASSERT(strcmp(result1->scoreExplain->children[1].str, "Upstream2: Vector similarity = 0.92") == 0);

  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test mergeHybridWrapper function with RRF scoring and targetIndex = 1 (second result as target)
 */
int testMergeHybridWrapper_RRF_TargetIndex1() {
  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults with mock explanations
  SearchResult* result1 = createTestSearchResult(1);
  SearchResult* result2 = createTestSearchResult(2);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Create mock score explanations for both results
  result1->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result1->scoreExplain->str = rm_strdup("Upstream1: TF-IDF score = 0.85");
  result1->scoreExplain->numChildren = 0;
  result1->scoreExplain->children = NULL;

  result2->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result2->scoreExplain->str = rm_strdup("Upstream2: Vector similarity = 0.92");
  result2->scoreExplain->numChildren = 0;
  result2->scoreExplain->children = NULL;

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Create RRF scoring context
  HybridScoringContext scoringCtx = {0};
  scoringCtx.scoringType = HYBRID_SCORING_RRF;
  scoringCtx.rrfCtx.k = 60;

  // Set up ranks for RRF calculation
  double ranks[2] = {1.0, 2.0}; // rank 1 and rank 2

  // Test: Call mergeHybridWrapper with result2 as target (index 1)
  double rrfScore = mergeHybridWrapper(hybridResult, 1, ranks, &scoringCtx);

  // Verify: RRF score was calculated and returned
  ASSERT(rrfScore > 0.0);
  double expectedRRF = (1.0 / (60 + 1)) + (1.0 / (60 + 2)); // 1/61 + 1/62
  ASSERT(fabs(rrfScore - expectedRRF) < 0.0001); // Allow small floating point difference

  // Verify: Second SearchResult (target) has scoreExplain populated
  ASSERT(result2->scoreExplain != NULL);
  ASSERT(result2->scoreExplain->str != NULL);

  // Verify: Explanation string matches expected RRF formula
  ASSERT(strcmp(result2->scoreExplain->str, "RRF: 0.03: 1/(60+1) + 1/(60+2)") == 0);

  // Verify: Structure has 2 children
  ASSERT(result2->scoreExplain->numChildren == 2);
  ASSERT(result2->scoreExplain->children != NULL);

  // Verify: Children have no sub-children and contain original explanations
  ASSERT(result2->scoreExplain->children[0].numChildren == 0);
  ASSERT(result2->scoreExplain->children[1].numChildren == 0);
  ASSERT(strcmp(result2->scoreExplain->children[0].str, "Upstream1: TF-IDF score = 0.85") == 0);
  ASSERT(strcmp(result2->scoreExplain->children[1].str, "Upstream2: Vector similarity = 0.92") == 0);

  // Verify: First result's scoreExplain was nullified (ownership transferred)
  ASSERT(result1->scoreExplain == NULL);

  // Cleanup
  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test mergeHybridWrapper function with RRF scoring and single upstream result
 */
int testMergeHybridWrapper_RRF_SingleResult() {
  // Create HybridSearchResult with 1 upstream
  HybridSearchResult* hybridResult = HybridSearchResult_New(1);
  ASSERT(hybridResult != NULL);

  // Create single SearchResult with mock explanation
  SearchResult* result1 = createTestSearchResult(0);
  ASSERT(result1 != NULL);

  // Create mock score explanation for the single upstream
  result1->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result1->scoreExplain->str = rm_strdup("Single: Vector search score = 0.95");
  result1->scoreExplain->numChildren = 0;
  result1->scoreExplain->children = NULL;

  // Store result in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->hasResults[0] = true;

  // Create RRF scoring context
  HybridScoringContext scoringCtx = {0};
  scoringCtx.scoringType = HYBRID_SCORING_RRF;
  scoringCtx.rrfCtx.k = 60;

  // Set up rank for single result
  double ranks[1] = {1.0}; // rank 1

  // Test: Call mergeHybridWrapper with single result (index 0)
  double rrfScore = mergeHybridWrapper(hybridResult, 0, ranks, &scoringCtx);

  // Verify: RRF score was calculated correctly for single result
  ASSERT(rrfScore > 0.0);
  double expectedRRF = 1.0 / (60 + 1); // 1/61
  ASSERT(fabs(rrfScore - expectedRRF) < 0.0001);

  // Verify: First SearchResult has scoreExplain populated
  ASSERT(result1->scoreExplain != NULL);
  ASSERT(result1->scoreExplain->str != NULL);

  // Verify: Explanation string matches expected single RRF formula
  ASSERT(strcmp(result1->scoreExplain->str, "RRF: 0.02: 1/(60+1)") == 0);

  // Verify: Structure has 1 child
  ASSERT(result1->scoreExplain->numChildren == 1);
  ASSERT(result1->scoreExplain->children != NULL);

  // Verify: Child has no sub-children and contains original explanation
  ASSERT(result1->scoreExplain->children[0].numChildren == 0);
  ASSERT(strcmp(result1->scoreExplain->children[0].str, "Single: Vector search score = 0.95") == 0);

  HybridSearchResult_Free(hybridResult);
  return 0;
}

/**
 * Test mergeHybridWrapper function with Linear scoring
 */
int testMergeHybridWrapper_Linear() {
  // Create HybridSearchResult with 2 upstreams
  HybridSearchResult* hybridResult = HybridSearchResult_New(2);
  ASSERT(hybridResult != NULL);

  // Create SearchResults with mock explanations
  SearchResult* result1 = createTestSearchResult(1);
  SearchResult* result2 = createTestSearchResult(2);
  ASSERT(result1 != NULL);
  ASSERT(result2 != NULL);

  // Create mock score explanations for both results
  result1->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result1->scoreExplain->str = rm_strdup("Upstream1: TF-IDF score = 0.85");
  result1->scoreExplain->numChildren = 0;
  result1->scoreExplain->children = NULL;

  result2->scoreExplain = rm_calloc(1, sizeof(RSScoreExplain));
  result2->scoreExplain->str = rm_strdup("Upstream2: Vector similarity = 0.92");
  result2->scoreExplain->numChildren = 0;
  result2->scoreExplain->children = NULL;

  // Store results in hybrid result
  hybridResult->searchResults[0] = result1;
  hybridResult->searchResults[1] = result2;
  hybridResult->hasResults[0] = true;
  hybridResult->hasResults[1] = true;

  // Create Linear scoring context
  HybridScoringContext scoringCtx = {0};
  scoringCtx.scoringType = HYBRID_SCORING_LINEAR;
  scoringCtx.linearCtx.numWeights = 2;
  scoringCtx.linearCtx.linearWeights = rm_malloc(2 * sizeof(double));
  scoringCtx.linearCtx.linearWeights[0] = 0.7;  // 70% weight for first score
  scoringCtx.linearCtx.linearWeights[1] = 0.3;  // 30% weight for second score

  // Set up scores for Linear calculation
  double scores[2] = {0.85, 0.92}; // raw scores

  // Test: Call mergeHybridWrapper with Linear scoring (result1 as target)
  double linearScore = mergeHybridWrapper(hybridResult, 0, scores, &scoringCtx);

  // Verify: Linear score was calculated and returned
  ASSERT(linearScore > 0.0);
  double expectedLinear = (0.7 * 0.85) + (0.3 * 0.92); // 0.595 + 0.276 = 0.871
  ASSERT(fabs(linearScore - expectedLinear) < 0.0001);

  // Verify: First SearchResult (target) has scoreExplain populated
  ASSERT(result1->scoreExplain != NULL);
  ASSERT(result1->scoreExplain->str != NULL);

  // Verify: Explanation string matches expected Linear formula
  ASSERT(strcmp(result1->scoreExplain->str, "Linear: 0.87: 0.70*0.85 + 0.30*0.92") == 0);

  // Verify: Structure has 2 children
  ASSERT(result1->scoreExplain->numChildren == 2);
  ASSERT(result1->scoreExplain->children != NULL);

  // Cleanup
  rm_free(scoringCtx.linearCtx.linearWeights);
  HybridSearchResult_Free(hybridResult);
  return 0;
}



TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testMergeFlags_NoFlags);
  TESTFUNC(testMergeFlags_ExpiredDoc);
  TESTFUNC(testUnionRLookupRows_SimpleUnion);
  TESTFUNC(testUnionRLookupRows_RefCounting);
  TESTFUNC(testUnionRLookupRows_OverlappingFields);
  TESTFUNC(testUnionRLookupRows_Idempotency);
  TESTFUNC(testMergeHybridWrapper_RRF_TargetIndex0);
  TESTFUNC(testMergeHybridWrapper_RRF_TargetIndex1);
  TESTFUNC(testMergeHybridWrapper_RRF_SingleResult);
  TESTFUNC(testMergeHybridWrapper_Linear);

})
