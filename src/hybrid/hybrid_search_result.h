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
#include "hybrid_lookup_context.h"
#include "vector_index.h"
#include "util/arr/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Per-query info needed to render the EXPLAINSCORE wrapper. The fields here
 * describe context that is constant across all yielded rows — per-doc values
 * (ranks, scores, scorer subtree) are filled in by the merger as it runs.
 */
typedef struct HybridExplainContext {
  // Owned. Resolved scorer name (defaults to RSGlobalConfig.defaultScorer when
  // the search sub-query did not specify one). Duplicated at build time so a
  // concurrent FT.CONFIG SET DEFAULT_SCORER cannot pull the string out from
  // under a long-running query while we render the explain wrapper.
  char *textScorerName;
  // Owned: e.g. "vector branch (KNN)" or "vector branch (RANGE: radius=…, epsilon=…)".
  char *vectorBranchEnvelope;
  // Vector retrieval mode of the VSIM sub-query.
  VectorQueryType vectorMode;
} HybridExplainContext;

void HybridExplainContext_Free(HybridExplainContext *ctx);

/**
 * Build an EXPLAINSCORE context from the two hybrid sub-AREQs.
 *
 * Reads the search sub-query's resolved scorer name and the vector
 * sub-query's retrieval mode (and, for RANGE, radius + optional epsilon
 * QueryAttribute) and freezes them into strings the merger reuses per row.
 *
 * Must be called after AREQ_ApplyContext on both sub-queries (it relies on the
 * resolved scorer name and the still-intact ParsedVectorData snapshot).
 * Returned context is owned by the caller.
 */
struct AREQ;
HybridExplainContext *HybridExplainContext_Build(const struct AREQ *searchReq, const struct AREQ *vectorReq);

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
 * Calculate hybrid score from multiple sources by combining their individual scores.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double calculateHybridScore(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx);

/**
 * Main function to merge SearchResults from multiple upstreams into a single comprehensive result.
 *
 * PRIMARY RESULT SELECTION:
 * The "primary result" is the first non-null SearchResult found in index order (0, 1, 2...).
 * This prefers search results (index 0) over vector results (index 1) when both exist for RSIndexResult.
 *
 * The primary result is the SearchResult we merge into and return to the downstream processor.
 * This function transfers ownership of the primary result from the HybridSearchResult to the caller.
 *
 * When `explainCtx` is non-NULL, an RSScoreExplain wrapper describing the
 * hybrid combine is attached to the merged result.
 */
SearchResult* mergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx,
                                 HybridLookupContext *lookupCtx, const HybridExplainContext *explainCtx);

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_SEARCH_RESULT_H__
