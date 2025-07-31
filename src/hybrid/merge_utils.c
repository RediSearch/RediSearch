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
 * Merge flags from all upstream SearchResults into the first SearchResult (in-place).
 */
void MergeFlags(HybridSearchResult *hybridResult) {
  RS_ASSERT(hybridResult)

  // Find the first valid SearchResult to merge into
  SearchResult *targetResult = NULL;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      targetResult = hybridResult->searchResults[i];
      break;
    }
  }

  if (!targetResult) return;

  // Merge flags from other upstreams into the first one (in-place)
  for (size_t i = 1; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      targetResult->flags |= hybridResult->searchResults[i]->flags;
    }
  }
}

/**
 * Simple RLookup union - merge fields from other upstreams into the first upstream's row (in-place).
 */
void UnionRLookupRows(HybridSearchResult *hybridResult, RLookupRow *targetRow, RLookup *lookup) {
  if (!hybridResult || !targetRow || !lookup) {
    return;
  }

  // In-place union: merge fields from upstreams 1,2,3... into upstream 0's row
  // Skip upstream 0 since targetRow is typically upstream 0's row
  for (size_t i = 1; i < hybridResult->numSources; i++) {
    if (!hybridResult->hasResults[i] || !hybridResult->searchResults[i]) continue;

    RLookupRow *upstreamRow = &hybridResult->searchResults[i]->rowdata;

    // Union all fields from this upstream into target row
    for (const RLookupKey *key = lookup->head; key; key = key->next) {
      if (!key->name) continue;  // Skip overridden keys

      RSValue *upstreamValue = RLookup_GetItem(key, upstreamRow);
      if (!upstreamValue) continue;  // Skip if upstream doesn't have this field

      RSValue *existingValue = RLookup_GetItem(key, targetRow);
      if (!existingValue) {
        // Field doesn't exist in target - add it
        RLookup_WriteKey(key, targetRow, upstreamValue);
      } else {
        // Field exists - assert that values are the same (our assumption)
        // This validates that "first upstream wins" == "no conflict resolution needed"
        QueryError err;
        QueryError_Init(&err);
        int equal = RSValue_Equal(existingValue, upstreamValue, &err);
        QueryError_ClearError(&err);  // Clean up any error details
        RS_ASSERT(equal == 1);
      }
    }
  }
}


/**
 * RRF wrapper - computes RRF score and populates explanation (in-place merging)
 * Works with HybridSearchResult and merges into the first SearchResult
 */
double mergeRRFWrapper(HybridSearchResult *hybridResult, double *ranks, size_t k, HybridScoringContext *scoringCtx) {
  if (!hybridResult || !ranks || !scoringCtx) {
    return 0.0;
  }

  // Find the first valid SearchResult to merge into
  SearchResult *targetResult = NULL;
  int numResults = 0;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      if (!targetResult) {
        targetResult = hybridResult->searchResults[i];
      }
      numResults++;
    }
  }

  if (!targetResult || numResults == 0) return 0.0;

  RSScoreExplain *scrExp = rm_calloc(1, sizeof(RSScoreExplain));
  scrExp->children = rm_calloc(numResults, sizeof(RSScoreExplain));
  scrExp->numChildren = numResults;

  // Copy children explanations from all upstreams and take ownership
  int childIdx = 0;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i] &&
        hybridResult->searchResults[i]->scoreExplain) {
      scrExp->children[childIdx] = *hybridResult->searchResults[i]->scoreExplain;

      // Nullify the source scoreExplain since we took ownership
      hybridResult->searchResults[i]->scoreExplain = NULL;

      childIdx++;
    }
  }

  // Calculate RRF score
  bool *hasRanks = rm_malloc(numResults * sizeof(bool));
  for (int i = 0; i < numResults; i++) {
    hasRanks[i] = true;
  }
  double rrf = HybridRRFScore(scoringCtx, ranks, hasRanks, numResults);

  // Create explanation string
  if (numResults == 2) {
    EXPLAIN(scrExp, "RRF: %.2f: 1/(%zu+%.0f) + 1/(%zu+%.0f)", rrf, k, ranks[0], k, ranks[1]);
  } else if (numResults == 1) {
    EXPLAIN(scrExp, "RRF: %.2f: 1/(%zu+%.0f)", rrf, k, ranks[0]);
  }

  // Populate the first SearchResult's scoreExplain (in-place)
  targetResult->scoreExplain = scrExp;
  rm_free(hasRanks);
  return rrf;
}


