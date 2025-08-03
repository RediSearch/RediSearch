/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_search_result.h"
#include "merge_utils.h"
#include "rmutil/alloc.h"
#include "query_error.h"
#include "score_explain.h"
#include "hybrid_scoring.h"
#include <string.h>
#include <assert.h>

/**
 * Constructor for HybridSearchResult.
 * Allocates memory for storing SearchResults from numUpstreams sources.
 */
HybridSearchResult* HybridSearchResult_New(size_t numUpstreams) {
  if (numUpstreams == 0) {
    return NULL;
  }

  HybridSearchResult* result = rm_calloc(1, sizeof(HybridSearchResult));
  result->searchResults = array_newlen(SearchResult*, numUpstreams);
  result->hasResults = array_newlen(bool, numUpstreams);
  result->numSources = numUpstreams;
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
 * Apply hybrid scoring to merge search results and generate explanations.
 * Computes hybrid score and populates explanation in target SearchResult.
 * Merges score explanations from source SearchResults into target.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double ApplyHybridScoring(HybridSearchResult *hybridResult, int8_t targetIndex, double *values, HybridScoringContext *scoringCtx) {
  if (!hybridResult || !values || !scoringCtx) {
    return 0.0;
  }
  RS_ASSERT(hybridResult->hasResults[targetIndex]);

  SearchResult *target = hybridResult->searchResults[targetIndex];

  // Count valid sources using hasResults flags
  int numValidSources = 0;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      numValidSources++;
    }
  }

  if (numValidSources == 0) return 0.0;

  RSScoreExplain *scrExp = rm_calloc(1, sizeof(RSScoreExplain));
  scrExp->children = rm_calloc(numValidSources, sizeof(RSScoreExplain));
  scrExp->numChildren = numValidSources;

  // Copy children explanations from all valid sources
  int childIdx = 0;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i] &&
        hybridResult->searchResults[i]->scoreExplain) {
      // Copy all data from source to child
      SECopy(&scrExp->children[childIdx], hybridResult->searchResults[i]->scoreExplain);

      // Clean up source explanation
      SEDestroy(hybridResult->searchResults[i]->scoreExplain);
      hybridResult->searchResults[i]->scoreExplain = NULL;

      childIdx++;
    }
  }

  // Calculate hybrid score using generic scoring function
  // Use the existing hasResults flags directly instead of allocating new array
  HybridScoringFunction scoringFunc = GetScoringFunction(scoringCtx->scoringType);
  double hybridScore = scoringFunc(scoringCtx, values, hybridResult->hasResults, hybridResult->numSources);

  // Create explanation string using function pointer
  HybridExplainFunction explainFunc = GetExplainFunction(scoringCtx->scoringType);
  explainFunc(scrExp, scoringCtx, values, hybridScore, numValidSources);

  // Target's old explanation was already copied to children and destroyed above
  // Just need to assign the new merged explanation

  // Populate the target SearchResult's scoreExplain (in-place)
  target->scoreExplain = scrExp;
  return hybridScore;
}

/**
 * Main function to merge SearchResults from multiple upstreams into a single comprehensive result.
 * Finds the primary result, computes hybrid score, merges flags, and returns the merged result.
 * This function transfers ownership of the primary result from the HybridSearchResult to the caller.
 * After calling this function, the caller is responsible for freeing the returned SearchResult.
 */
SearchResult* MergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx) {
  if (!hybridResult || !scoringCtx) {
    return NULL;
  }

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


  // Transfer ownership: Remove primary result from HybridSearchResult to prevent double-free
  hybridResult->searchResults[targetIndex] = NULL;
  hybridResult->hasResults[targetIndex] = false;

  return primary;
}

