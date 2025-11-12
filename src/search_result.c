/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "search_result.h"
#include "redisearch.h"
#include "redisearch_rs/headers/types_rs.h"
#include "rlookup.h"
#include "score_explain.h"

// Allocates a new SearchResult, and populates it with `r`'s data (takes
// ownership as well)
SearchResult *SearchResult_AllocateMove(SearchResult *r) {
  SearchResult *ret = rm_malloc(sizeof(*ret));
  *ret = *r;
  return ret;
}

void SearchResult_Clear(SearchResult *r) {
  // This won't affect anything if the result is null

  SearchResult_SetScore(r, 0.0);

  RSScoreExplain *score_explain = SearchResult_GetScoreExplainMut(r);
  if (score_explain) {
    SEDestroy(score_explain);
    SearchResult_SetScoreExplain(r, NULL);
  }

  const RSIndexResult* index_result = SearchResult_GetIndexResult(r);
  if (index_result) {
    SearchResult_SetIndexResult(r, NULL);
  }

  SearchResult_SetFlags(r, 0);
  RLookupRow_Wipe(SearchResult_GetRowDataMut(r));

  const RSDocumentMetadata* dmd = SearchResult_GetDocumentMetadata(r);
  if (dmd) {
    DMD_Return(dmd);
    SearchResult_SetDocumentMetadata(r, NULL);
  }
}

void SearchResult_Destroy(SearchResult *r) {
  SearchResult_Clear(r);
  RLookupRow_Reset(SearchResult_GetRowDataMut(r));
}

void SearchResult_Override(SearchResult *dst, SearchResult *src) {
  if (!src) return;
  RLookupRow oldrow = dst->rowdata;
  *dst = *src;
  RLookupRow_Reset(&oldrow);
}
