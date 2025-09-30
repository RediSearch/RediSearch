/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "score_explain.h"
#include "redisearch.h"
#include "types_rs.h"
#include "rlookup.h"
#include "index_result.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * SearchResult - the object all the processing chain is working on.
 * It has the indexResult which is what the index scan brought - scores, vectors, flags, etc,
 * and a list of fields loaded by the chain
 */
typedef struct {
  t_docId docId;

  // not all results have score - TBD
  double score;
  RSScoreExplain *scoreExplain;

  RSDocumentMetadata *dmd;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  const RSIndexResult* indexResult;

  // Row data. Use RLookup_* functions to access
  RLookupRow rowdata;

  uint8_t flags;
} SearchResult;

/* SearchResult flags */
static const uint8_t Result_ExpiredDoc = 1 << 0;

/**
 * Moves the contents of `r` into a newly heap-allocated SearchResult.
 * This function takes ownership of the search result, so `r` **must not** be used after this
 * function is called.
 */
SearchResult* SearchResult_AllocateMove(SearchResult* r);

/**
 * This function resets the search result, so that it may be reused again.
 * Internal caches are reset but not freed
 */
void SearchResult_Clear(SearchResult* r);

/**
 * This function clears the search result, also freeing its internals. Internal
 * caches are freed. Use this function if `r` will not be used again.
 */
void SearchResult_Destroy(SearchResult* r);

/**
 * Overwrites the contents of 'dst' with those from 'src'.
 * Ensures proper cleanup of any existing data in 'dst'.
 */
void SearchResult_Override(SearchResult* dst, SearchResult* src);

/**
 * Returns the document ID of `res`.
*/
static inline t_docId SearchResult_GetDocId(const SearchResult* res) {
  return res->docId;
}

/**
 * Sets the document ID of `res`.
 */
static inline void SearchResult_SetDocId(SearchResult* res, t_docId docId) {
  res->docId = docId;
}

/**
 * Returns the score of `res`.
 */
static inline double SearchResult_GetScore(const SearchResult* res) {
  return res->score;
}

/**
 * Sets the score of `res`.
 */
static inline void SearchResult_SetScore(SearchResult* res, double score) {
  res->score = score;
}

/**
 * Returns an immutable pointer to the `RSScoreExplain` associated with `res`.
 * If you need to mutate the `RSScoreExplain` consider using `SearchResult_GetScoreExplainMut` instead.
 */
static inline const RSScoreExplain* SearchResult_GetScoreExplain(const SearchResult* res) {
  return res->scoreExplain;
}

/**
 * Returns a mutable pointer to the [`ffi::RSScoreExplain`] associated with `res`.
 * If you do not need to mutate the `RSScoreExplain` consider using `SearchResult_GetScoreExplain` instead.
 */
static inline RSScoreExplain* SearchResult_GetScoreExplainMut(SearchResult* res) {
  return res->scoreExplain;
}

/**
 * Sets the `RSScoreExplain` associated with `res`.
 */
static inline void SearchResult_SetScoreExplain(SearchResult* res, RSScoreExplain* scoreExplain) {
  res->scoreExplain = scoreExplain;
}

/**
 * Returns an immutable pointer to the `RSDocumentMetadata` associated with `res`.
 */
static inline const RSDocumentMetadata* SearchResult_GetDocumentMetadata(const SearchResult* res) {
  return res->dmd;
}

/**
 * Sets the `RSDocumentMetadata` associated with `res`.
 */
static inline void SearchResult_SetDocumentMetadata(SearchResult* res,
                                                    RSDocumentMetadata* dmd) {
  res->dmd = dmd;
}

/**
 * Returns an immutable pointer to the [RSIndexResult` associated with `res`.
 */
static inline const RSIndexResult* SearchResult_GetIndexResult(const SearchResult* res) {
  return res->indexResult;
}

/**
 * Returns true if `res` has an associated `RSIndexResult`.
 */
static inline bool SearchResult_HasIndexResult(const SearchResult* res) {
  return res->indexResult;
}

/**
 * Sets the `RSIndexResult` associated with `res`.
 */
static inline void SearchResult_SetIndexResult(SearchResult* res, RSIndexResult* indexResult) {
  res->indexResult = indexResult;
}

/**
 * Returns an immutable pointer to the [`RLookupRow`] of `res`.
 * If you need to mutate the `RLookupRow` consider using `SearchResult_GetRowDataMut` instead.
 */
static inline const RLookupRow* SearchResult_GetRowData(const SearchResult* res) {
  return &res->rowdata;
}

/**
 * Returns a mutable pointer to the `RLookupRow` of `res`.
 * If you dont need to mutate the `RLookupRow` consider using `SearchResult_GetRowData` instead.
 */
static inline RLookupRow* SearchResult_GetRowDataMut(SearchResult* res) {
  return &res->rowdata;
}

/**
 * Returns the `SearchResultFlags` of `res`.
 */
static inline uint8_t SearchResult_GetFlags(const SearchResult* res) {
  return res->flags;
}

/**
 * Sets the [`SearchResultFlags`] of `res`.
 */
static inline void SearchResult_SetFlags(SearchResult* res, uint8_t flags) {
  res->flags = flags;
}

/**
 * Merges flags (union) from `res` and `other` into `res`
 */
static inline void SearchResult_MergeFlags(SearchResult* res, const SearchResult* other) {
  RS_ASSERT(res && other);
  res->flags |= other->flags;
}

#ifdef __cplusplus
}
#endif
