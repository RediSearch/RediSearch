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
 * Merge flags from all upstream SearchResults.
 */
uint8_t MergeFlags(HybridSearchResult *hybridResult);

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_MERGE_UTILS_H__
