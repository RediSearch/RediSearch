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
#include <string.h>

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
 * Merge flags from all upstream SearchResults using union-like logic.
 * Currently handles Result_ExpiredDoc flag with OR operation.
 */
uint8_t MergeFlags(HybridSearchResult *hybridResult) {
  if (!hybridResult) {
    return 0;
  }

  uint8_t mergedFlags = 0;

  // Currently only one SearchResult flag is defined: Result_ExpiredDoc
  // This flag should be set if ANY upstream indicates the document is expired
  // Using OR operation which satisfies union properties:

  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && hybridResult->searchResults[i]) {
      mergedFlags |= hybridResult->searchResults[i]->flags;
    }
  }

  return mergedFlags;
}
