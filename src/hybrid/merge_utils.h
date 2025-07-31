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
 * Merge flags from source flags into target flags (in-place).
 * Modifies target_flags by ORing it with source_flags.
 */
void MergeFlags(uint8_t *target_flags, const uint8_t *source_flags);

/**
 * Simple RLookup union - copy fields from source row to target row.
 * No conflict resolution is performed, assuming no conflicts (all keys have same values)
 */
void UnionRLookupRows(RLookupRow *target_row, const RLookupRow *source_row, const RLookup *lookup);

/**
 * RRF wrapper that computes RRF score and populates explanation in target SearchResult.
 * Merges score explanations from source SearchResults into target.
 */
double mergeRRFWrapper(SearchResult **sources, size_t numSources, size_t targetIndex, double *ranks, size_t k, HybridScoringContext *scoringCtx);



#ifdef __cplusplus
}
#endif

#endif // __HYBRID_MERGE_UTILS_H__
