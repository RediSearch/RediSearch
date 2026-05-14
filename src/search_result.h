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
 * Sets the [`RSIndexResult`] associated with `res` as a *borrow*.
 *
 * `res` does not take ownership of `index_result`; the caller must keep it
 * alive for as long as `res` references it. After this call,
 * `Result_OwnsIndexResult` is cleared.
 *
 * Passing `NULL` clears the field (and frees any prior owned copy).
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. If non-NULL, `index_result` must be a [valid] pointer to an
 *    [`ffi::RSIndexResult`] that outlives every dereference of `res`'s
 *    `_index_result` field.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetBorrowedIndexResult(SearchResult *res, const RSIndexResult *index_result) {
  if ((res->_flags & Result_OwnsIndexResult) && res->_index_result != index_result) {
    IndexResult_Free((RSIndexResult *)res->_index_result);
  }
  res->_index_result = index_result;
  res->_flags &= ~Result_OwnsIndexResult;
}

/**
 * Sets the [`RSIndexResult`] associated with `res` as an *owned* allocation.
 *
 * `res` takes ownership of `index_result` and `Result_OwnsIndexResult` is
 * set, so `SearchResult_Clear` / `SearchResult_Destroy` will release it via
 * `IndexResult_Free`. The caller must not free `index_result` itself.
 *
 * If `res` previously owned its `_index_result`, the prior owned copy is
 * freed before the new pointer is installed.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `index_result` must be a non-NULL pointer to an [`ffi::RSIndexResult`]
 *    allocated by one of the constructors documented on
 *    [`IndexResult_Free`] (e.g. `IndexResult_DeepCopy`).
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline void SearchResult_SetOwnedIndexResult(SearchResult *res, RSIndexResult *index_result) {
  if ((res->_flags & Result_OwnsIndexResult) && res->_index_result != index_result) {
    IndexResult_Free((RSIndexResult *)res->_index_result);
  }
  res->_index_result = index_result;
  res->_flags |= Result_OwnsIndexResult;
}

/**
 * Promote the borrowed `_index_result` of `res` to an owned deep copy.
 *
 * Idempotent: if `res` has no `_index_result`, or it already owns one
 * (`Result_OwnsIndexResult` is set), this is a no-op. Otherwise the existing
 * borrow is replaced with `IndexResult_DeepCopy(borrow)` and the
 * `Result_OwnsIndexResult` flag is set so that `SearchResult_Clear` /
 * `SearchResult_Destroy` will free the copy.
 *
 * Call this in any pipeline stage that buffers a `SearchResult` across an
 * iterator advance — the borrow into `it->current` will dangle once the
 * iterator is read again.
 *
 * # Safety
 *
 * 1. `res` must be a valid, non-null pointer to a `SearchResult`.
 * 2. If `_index_result` is non-NULL it must currently be a valid pointer
 *    (either a live borrow or an already-owned copy).
 */
static inline void SearchResult_DeepCopyAndOwnIndexResult(SearchResult *res) {
  if (!res->_index_result) return;
  if (res->_flags & Result_OwnsIndexResult) return;
  SearchResult_SetOwnedIndexResult(res, IndexResult_DeepCopy(res->_index_result));
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
 * Merge the flags (union) of `other` into `res`.
 *
 * Only document-semantic flags are propagated. Ownership/lifecycle flags
 * (e.g. `Result_OwnsIndexResult`) describe a property of *this* `SearchResult`'s
 * own allocations and must never be inherited from another result — doing so
 * would cause `SearchResult_Clear` / `SearchResult_Destroy` to free memory
 * that `res` does not actually own.
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
  // Flags that describe per-result memory ownership — must NOT be merged.
  const uint8_t ownership_flags = Result_OwnsIndexResult;
  res->_flags |= (other->_flags & ~ownership_flags);
}

#ifdef __cplusplus
}
#endif
