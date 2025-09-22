/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "result_processor.h"
#include "hybrid_search_result.h"
#include "rmutil/alloc.h"
#include "query_error.h"
#include "score_explain.h"
#include "hybrid_scoring.h"
#include <string.h>
#include <assert.h>

/**
 * Merge flags from source into target (in-place).
 * Modifies target by ORing it with source flags.
 */
void mergeFlags(uint8_t *target_flags, const uint8_t *source_flags) {
  RS_ASSERT(target_flags && source_flags);
  *target_flags |= *source_flags;
}

/**
 * Constructor for HybridSearchResult.
 * Allocates memory for storing SearchResults from numSources sources.
 */
HybridSearchResult* HybridSearchResult_New(size_t numSources) {
  RS_ASSERT(numSources > 0);

  HybridSearchResult* result = rm_calloc(1, sizeof(HybridSearchResult));
  result->searchResults = array_newlen(SearchResult*, numSources);
  result->hasResults = array_newlen(bool, numSources);
  for (size_t i = 0; i < numSources; ++i) {
    result->searchResults[i] = NULL;
    result->hasResults[i] = false;
  }
  result->numSources = numSources;
  return result;
}

/**
 * Destructor for HybridSearchResult.
 * Frees all stored SearchResults and the structure itself.
 */
void HybridSearchResult_Free(HybridSearchResult* result) {
  if (!result) {
    return;
  }

  // Free individual SearchResults with array_free_ex
  array_free_ex(result->searchResults, {
    SearchResult **sr = (SearchResult**)ptr;
    if (*sr) {
      SearchResult_Destroy(*sr);
      rm_free(*sr);
    }
  });

  array_free(result->hasResults);

  rm_free(result);
}

/**
 * Store a SearchResult from a source into the HybridSearchResult.
 * Updates the score of the SearchResult and marks the source as having results.
 */
void HybridSearchResult_StoreResult(HybridSearchResult* hybridResult, SearchResult* searchResult, int sourceIndex) {
  RS_ASSERT(hybridResult && searchResult);
  RS_ASSERT(sourceIndex < hybridResult->numSources);

  // Store the SearchResult from this source (preserving all data)
  hybridResult->searchResults[sourceIndex] = searchResult;
  hybridResult->hasResults[sourceIndex] = true;
}


/**
 * Calculate hybrid score from multiple sources by combining their individual scores.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double calculateHybridScore(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx) {
  RS_ASSERT(scoringCtx && hybridResult)

  // Extract values from SearchResults
  arrayof(double) values = array_newlen(double, hybridResult->numSources);
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      RS_ASSERT(hybridResult->searchResults[i]);
      // Note: SearchResult->score contains ranks for RRF, scores for Linear
      // This is set correctly by upstream processors based on scoring type
      values[i] = hybridResult->searchResults[i]->score;
    } else {
      values[i] = 0.0;  // Default value for missing results
    }
  }

  // Calculate hybrid score using generic scoring function
  HybridScoringFunction scoringFunc = GetScoringFunction(scoringCtx->scoringType);
  double result = scoringFunc(scoringCtx, values, hybridResult->hasResults, hybridResult->numSources);

  array_free(values);
  return result;
}

/**
 * Merge field data from multiple source SearchResults into destination RLookupRow.
 * Initializes destination row and writes fields from each source using RLookupRow_WriteFieldsFrom.
 */
static void merge_rlookuprows(HybridSearchResult *hybridResult,
                            HybridLookupContext *lookupCtx,
                            RLookupRow *destination) {
  RS_ASSERT(hybridResult && lookupCtx && destination);

  // Initialize destination row
  RLookupRow_Wipe(destination);

  // Write fields from each source SearchResult
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      SearchResult *sourceResult = hybridResult->searchResults[i];
      RS_ASSERT(sourceResult);

      // Write fields from source row to destination row
      RLookupRow_WriteFieldsFrom(&sourceResult->rowdata, lookupCtx->sourceLookups[i],
                                destination, lookupCtx->tailLookup);
    }
  }
}

/**
 * Main function to merge SearchResults from multiple upstreams into a single comprehensive result.
 *
 * PRIMARY RESULT SELECTION:
 * The "primary result" is the first non-null SearchResult found in index order (0, 1, 2...).
 * This prefers search results (index 0) over vector results (index 1) when both exist for RSIndexResult.
 *
 * The primary result is the SearchResult we merge into and return to the downstream processor.
 * This function transfers ownership of the primary result from the HybridSearchResult to the caller.
 */
SearchResult* mergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx, HybridLookupContext *lookupCtx) {
  RS_ASSERT(hybridResult && scoringCtx);

  // Find the primary result (first non-null result)
  SearchResult *primary = NULL;
  int8_t targetIndex = -1;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      RS_ASSERT(hybridResult->searchResults[i]);
      primary = hybridResult->searchResults[i];
      targetIndex = (int8_t)i;
      break;
    }
  }

  RS_ASSERT(primary && targetIndex != -1);

  // Calculate hybrid score by combining scores from all sources
  primary->score = calculateHybridScore(hybridResult, scoringCtx);

  // Merge flags from all upstreams
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && i != targetIndex) {
      RS_ASSERT(hybridResult->searchResults[i]);
      mergeFlags(&primary->flags, &hybridResult->searchResults[i]->flags);
    }
  }
  // Merge field data into primary result's rowdata
  // Create temporary row for merging (avoids modifying primary while reading from it)
  RLookupRow tempRow = {0};  // Stack allocation, zero-initialized
  merge_rlookuprows(hybridResult, lookupCtx, &tempRow);

  // Prepare primary row and move merged data from temporary row
  RLookupRow_Wipe(&primary->rowdata);  // Clear primary row
  RLookupRow_Move(lookupCtx->tailLookup, &tempRow, &primary->rowdata);  // Move temp → primary
  RLookupRow_Reset(&tempRow);
  // Transfer ownership: Remove primary result from HybridSearchResult to prevent double-free
  hybridResult->searchResults[targetIndex] = NULL;
  hybridResult->hasResults[targetIndex] = false;

  return primary;
}

