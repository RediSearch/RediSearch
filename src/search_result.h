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
#include "search_result_rs.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline SearchResult SearchResult_New() {
    SearchResult r = {0};
    return r;
}

static inline SearchResult SearchResult_New_2(const RLookup *lookup) {
    SearchResult r = {0};
    r._row_data = RLookupRow_New(lookup);
    return r;
}

/**
 * Returns the document ID of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline t_docId SearchResult_GetDocId(const SearchResult *res) {
  return res->_doc_id;
}

/**
 * Sets the document ID of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetDocId(SearchResult *res, t_docId doc_id) {
  res->_doc_id = doc_id;
}

/**
 * Returns the score of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline double SearchResult_GetScore(const SearchResult *res) {
  return res->_score;
}

/**
 * Sets the score of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetScore(SearchResult *res, double score)  {
  res->_score = score;
}

/**
 * Returns an immutable pointer to the [`ffi::RSScoreExplain`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline const RSScoreExplain *SearchResult_GetScoreExplain(const SearchResult *res)  {
  return res->_score_explain;
}

/**
 * Returns a mutable pointer to the [`ffi::RSScoreExplain`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline RSScoreExplain *SearchResult_GetScoreExplainMut(SearchResult *res) {
  return res->_score_explain;
}

/**
 * Sets the [`ffi::RSScoreExplain`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `score_explain` must be a [valid] pointer to a [`ffi::RSScoreExplain`].
 * 3. `score_explain` must be [valid] for the entire lifetime of `res`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetScoreExplain(SearchResult *res, RSScoreExplain *score_explain) {
  res->_score_explain = score_explain;
}

/**
 * Returns an immutable reference to the [`ffi::RSDocumentMetadata`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline const RSDocumentMetadata *SearchResult_GetDocumentMetadata(const SearchResult *res) {
  return res->_document_metadata;
}

/**
 * Sets the [`ffi::RSDocumentMetadata`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `document_metadata` must be a [valid] pointer to a [`ffi::RSDocumentMetadata`].
 * 3. `document_metadata` must be not be mutated for the entire lifetime of `res`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetDocumentMetadata(SearchResult *res,
                                      const RSDocumentMetadata *document_metadata) {
                                        res->_document_metadata = document_metadata;
                                      }

/**
 * Returns an immutable pointer to the [`RSIndexResult`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline const RSIndexResult *SearchResult_GetIndexResult(const SearchResult *res) {
  return res->_index_result;
}

/**
 * Sets the [`RSIndexResult`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline bool SearchResult_HasIndexResult(const SearchResult *res) {
  return res->_index_result;
}

/**
 * Sets the [`RSIndexResult`] associated with `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `index_result` must be a [valid] pointer to a [`ffi::RSIndexResult`].
 * 3. `index_result` must be [valid] for the entire lifetime of `res`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetIndexResult(SearchResult *res, const RSIndexResult *index_result) {
  res->_index_result = index_result;
}

/**
 * Returns an immutable pointer to the [`RLookupRow`][ffi::RLookupRow] of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline const RLookupRow *SearchResult_GetRowData(const SearchResult *res) {
  return &res->_row_data;
}

/**
 * Returns a mutable pointer to the [`RLookupRow`][ffi::RLookupRow] of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline RLookupRow *SearchResult_GetRowDataMut(SearchResult *res) {
  return &res->_row_data;
}

/**
 * Sets the [`RLookupRow`][ffi::RLookupRow] of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a correctly initialized [`RLookupRow`][ffi::RLookupRow].
 */
static inline void SearchResult_SetRowData(SearchResult *res, RLookupRow row_data) {
    res->_row_data = row_data;
}

/**
 * Returns the [`SearchResultFlags`] of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline uint8_t SearchResult_GetFlags(const SearchResult *res) {
  return res->_flags;
}

/**
 * Sets the [`SearchResultFlags`] of `res`.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetFlags(SearchResult *res, uint8_t flags) {
  res->_flags = flags;
}

/**
 * Merge the flags (union) `other` into `res`
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `other` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_MergeFlags(SearchResult *res, const SearchResult *other)  {
  RS_ASSERT(res && other);
  res->_flags |= other->_flags;
}

#ifdef __cplusplus
}
#endif
