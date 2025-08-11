/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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
void MergeFlags(uint8_t *target_flags, const uint8_t *source_flags) {
  if (!target_flags || !source_flags) {
    return;
  }

  *target_flags |= *source_flags;
}

/**
 * Constructor for HybridSearchResult.
 * Allocates memory for storing SearchResults from numSources sources.
 */
HybridSearchResult* HybridSearchResult_New(size_t numSources) {
  if (numSources == 0) {
    return NULL;
  }

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
  if (!hybridResult || !searchResult ) {
    return;
  }
  RS_ASSERT(sourceIndex < hybridResult->numSources);

  // Store the SearchResult from this source (preserving all data)
  hybridResult->searchResults[sourceIndex] = searchResult;
  hybridResult->hasResults[sourceIndex] = true;
}


/**
 * Apply hybrid scoring to compute combined score from multiple sources.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double ApplyHybridScoring(HybridSearchResult *hybridResult, int8_t targetIndex, double *values, HybridScoringContext *scoringCtx) {
  if (!hybridResult || !values || !scoringCtx) {
    return 0.0;
  }
  RS_ASSERT(hybridResult->hasResults[targetIndex]);

  // Calculate hybrid score using generic scoring function
  HybridScoringFunction scoringFunc = GetScoringFunction(scoringCtx->scoringType);
  return scoringFunc(scoringCtx, values, hybridResult->hasResults, hybridResult->numSources);
}

/**
 * Main function to merge SearchResults from multiple upstreams into a single comprehensive result.
 * Finds the primary result, computes hybrid score, merges flags, and returns the merged result.
 * This function transfers ownership of the primary result from the HybridSearchResult to the caller.
 */
SearchResult* MergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx) {
  RS_ASSERT(hybridResult && scoringCtx);

  // Find the primary result (first non-null result)
  SearchResult *primary = NULL;
  int8_t targetIndex = -1;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      primary = hybridResult->searchResults[i];
      targetIndex = (int8_t)i;
      break;
    }
  }

  if (!primary || targetIndex == -1) {
    return NULL;
  }

  // Prepare scores array for hybrid scoring using array.h
  arrayof(double) scores = array_newlen(double, hybridResult->numSources);
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      scores[i] = hybridResult->searchResults[i]->score;
    } else {
      scores[i] = 0.0;  // Default score for missing results
    }
  }

  // Apply hybrid scoring to compute hybrid score and merge explanations
  double hybridScore = ApplyHybridScoring(hybridResult, targetIndex, scores, scoringCtx);
  array_free(scores);

  // Update primary result's score
  primary->score = hybridScore;

  // Merge flags from all upstreams
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i] && i != targetIndex) {
      MergeFlags(&primary->flags, &hybridResult->searchResults[i]->flags);
    }
  }

  // RLookup rows – use the primary result's RLookupRow. Assumes that in FT.HYBRID, all RLookups are synchronized
  // (required keys exist in all of them and reference the same row indices).

  // Transfer ownership: Remove primary result from HybridSearchResult to prevent double-free
  hybridResult->searchResults[targetIndex] = NULL;
  hybridResult->hasResults[targetIndex] = false;

  return primary;
}

