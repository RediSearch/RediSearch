/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef FILTER_SUMMARY_H__
#define FILTER_SUMMARY_H__

#include "filter_quick_check.h"

struct RSAddDocumentCtx;
struct IndexSpec;
typedef struct FilterSummary FilterSummary;

#ifdef __cplusplus
extern "C" {
#endif

/* RAM POC: summarize every indexed TAG, TEXT and NUMERIC field after indexing.
 * The caller holds the spec write lock; failed/incomplete versions stay unavailable. */
void FilterSummary_IndexDocument(const struct RSAddDocumentCtx* aCtx);
size_t FilterSummary_MemoryUsage(const FilterSummary* summary);
void FilterSummary_Free(FilterSummary* summary);

typedef struct {
  struct IndexSpec* spec;
  FilterQuickCheck* check;
  size_t schemaFieldCount;
} FilterSummaryContext;

/* Owns the compiled check; borrows spec. Hold its read lock during creation and evaluation.
 * Evaluations borrow at most one document; no document or summary pointer survives the call. */
FilterSummaryContext FilterSummary_CreateContext(struct IndexSpec* spec,
                                                 const FilterQuickCheckPredicate* predicate);
void FilterSummary_FreeContext(FilterSummaryContext* context);
int FilterSummary_MayMatch(void* context, size_t label);

#ifdef __cplusplus
}
#endif
#endif
