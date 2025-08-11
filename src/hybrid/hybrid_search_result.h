/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef __HYBRID_SEARCH_RESULT_H__
#define __HYBRID_SEARCH_RESULT_H__

#include "result_processor.h"
#include "hybrid_scoring.h"
#include "util/arr/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * HybridSearchResult structure that stores SearchResults from multiple sources.
 */
typedef struct {
  arrayof(SearchResult*) searchResults;  // Array of SearchResults from each source
  arrayof(bool) hasResults;              // Result availability flags
  size_t numSources;                     // Number of sources
} HybridSearchResult;

/**
 * Constructor for HybridSearchResult.
 */
HybridSearchResult* HybridSearchResult_New(size_t numSources);

/**
 * Destructor for HybridSearchResult.
 */
void HybridSearchResult_Free(HybridSearchResult* result);

/**
 * Store a SearchResult from a source into the HybridSearchResult.
 * Updates the score of the SearchResult and marks the source as having results.
 */
void HybridSearchResult_StoreResult(HybridSearchResult* hybridResult, SearchResult* searchResult, int sourceIndex);

/**
 * Merge flags from source flags into target flags (in-place).
 * Modifies target_flags by ORing it with source_flags.
 */
void MergeFlags(uint8_t *target_flags, const uint8_t *source_flags);

/**
 * Apply hybrid scoring to compute combined score from multiple sources.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double ApplyHybridScoring(HybridSearchResult *hybridResult, int8_t targetIndex, double *values, HybridScoringContext *scoringCtx);

/**
 * Main function to merge SearchResults from multiple upstreams into a single comprehensive result.
 * Finds the primary result, computes hybrid score, merges flags, and returns the merged result.
 * This function transfers ownership of the primary result away from the HybridSearchResult.
 */
SearchResult* MergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx);



#ifdef __cplusplus
}
#endif

#endif // __HYBRID_SEARCH_RESULT_H__