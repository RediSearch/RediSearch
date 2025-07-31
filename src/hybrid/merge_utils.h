/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef __HYBRID_MERGE_UTILS_H__
#define __HYBRID_MERGE_UTILS_H__

#include "result_processor.h"
#include "hybrid_scoring.h"
#include "util/arr/arr.h"
#include "score_explain.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * HybridSearchResult structure that stores SearchResults from multiple upstreams.
 */
typedef struct {
  arrayof(SearchResult*) searchResults;  // Array of SearchResults from each upstream
  arrayof(bool) hasResults;              // Result availability flags
  size_t numSources;                     // Number of upstreams
} HybridSearchResult;

/**
 * Constructor for HybridSearchResult.
 */
HybridSearchResult* HybridSearchResult_New(size_t numUpstreams);

/**
 * Destructor for HybridSearchResult.
 */
void HybridSearchResult_Free(HybridSearchResult* result);

/**
 * Merge flags from all upstream SearchResults into the first SearchResult (in-place).
 */
void MergeFlags(HybridSearchResult *hybridResult);

/**
 * Simple RLookup union - copy fields from all upstreams to merged row.
 * No conflict resolution is performed, assuming no conflicts (all keys have same values)
 */
void UnionRLookupRows(HybridSearchResult *hybridResult, RLookupRow *mergedRow, RLookup *lookup);

/**
 * RRF wrapper that computes RRF score and populates explanation (in-place merging).
 * Works with HybridSearchResult and merges into the first SearchResult.
 */
double mergeRRFWrapper(HybridSearchResult *hybridResult, double *ranks, size_t k, HybridScoringContext *scoringCtx);



#ifdef __cplusplus
}
#endif

#endif // __HYBRID_MERGE_UTILS_H__
