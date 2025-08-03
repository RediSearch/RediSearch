/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "merge_utils.h"
#include "rmutil/alloc.h"
#include "query_error.h"
#include "score_explain.h"
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
 * Union RLookup rows - copy fields from source row to target row.
 * No conflict resolution is performed, assuming no conflicts (all keys have same values).
 */
void UnionRLookupRows(RLookupRow *target_row, const RLookupRow *source_row, const RLookup *lookup) {
  if (!target_row || !source_row || !lookup) {
    return;
  }

  // Union all fields from source row into target row
  for (const RLookupKey *key = lookup->head; key; key = key->next) {
    if (!key->name) continue;  // Skip overridden keys

    RSValue *sourceValue = RLookup_GetItem(key, source_row);
    if (!sourceValue) continue;  // Skip if source doesn't have this field

    RSValue *existingValue = RLookup_GetItem(key, target_row);
    if (!existingValue) {
      // Field doesn't exist in target - add it
      RLookup_WriteKey(key, target_row, sourceValue);
    } else {
      // Field exists - assert that values are the same (our assumption)
      // This validates that "first upstream wins" == "no conflict resolution needed"
      QueryError err;
      QueryError_Init(&err);
      int equal = RSValue_Equal(existingValue, sourceValue, &err);
      QueryError_ClearError(&err);  // Clean up any error details
      RS_ASSERT(equal == 1);
    }
  }
}


/**
 * Generic hybrid wrapper - computes hybrid score and populates explanation in target SearchResult.
 * Merges score explanations from source SearchResults into target.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 * Uses HybridSearchResult which contains sources, hasResults flags, and numSources.
 */
double mergeHybridWrapper(HybridSearchResult *hybridResult, int8_t targetIndex, double *values, HybridScoringContext *scoringCtx) {
  RS_ASSERT(hybridResult && values && scoringCtx);
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
  bool *hasValues = rm_malloc(numValidSources * sizeof(bool));
  for (int i = 0; i < numValidSources; i++) {
    hasValues[i] = true;
  }
  HybridScoringFunction scoringFunc = GetScoringFunction(scoringCtx->scoringType);
  double hybridScore = scoringFunc(scoringCtx, values, hasValues, numValidSources);

  // Create explanation string based on scoring type
  if (scoringCtx->scoringType == HYBRID_SCORING_RRF) {
    if (numValidSources == 2) {
      EXPLAIN(scrExp, "RRF: %.2f: 1/(%zu+%.0f) + 1/(%zu+%.0f)", hybridScore, scoringCtx->rrfCtx.k, values[0], scoringCtx->rrfCtx.k, values[1]);
    } else if (numValidSources == 1) {
      EXPLAIN(scrExp, "RRF: %.2f: 1/(%zu+%.0f)", hybridScore, scoringCtx->rrfCtx.k, values[0]);
    }
  } else if (scoringCtx->scoringType == HYBRID_SCORING_LINEAR) {
    if (numValidSources == 2) {
      EXPLAIN(scrExp, "Linear: %.2f: %.2f*%.2f + %.2f*%.2f", hybridScore,
              scoringCtx->linearCtx.linearWeights[0], values[0],
              scoringCtx->linearCtx.linearWeights[1], values[1]);
    } else if (numValidSources == 1) {
      EXPLAIN(scrExp, "Linear: %.2f: %.2f*%.2f", hybridScore,
              scoringCtx->linearCtx.linearWeights[0], values[0]);
    }
  }

  // Target's old explanation was already copied to children and destroyed above
  // Just need to assign the new merged explanation

  // Populate the target SearchResult's scoreExplain (in-place)
  target->scoreExplain = scrExp;
  rm_free(hasValues);
  return hybridScore;
}


