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
        RS_ASSERT(equal == 0);
      }
    }
  }
}
