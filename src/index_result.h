/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "util/arr.h"
#include "value.h"
#include "rlookup.h"
#define DEFAULT_RECORDLIST_SIZE 4

#ifdef __cplusplus
extern "C" {
#endif

RSQueryTerm *NewQueryTerm(RSToken *tok, int id);
void Term_Free(RSQueryTerm *t);

void ResultMetrics_Concat(RSIndexResult *parent, RSIndexResult *child);

static inline void ResultMetrics_Add(RSIndexResult *r, RLookupKey *key, RSValue *val) {
  RSYieldableMetric new_element = {.key = key, .value = val};
  r->metrics = array_ensure_append_1(r->metrics, new_element);
}

static inline void ResultMetrics_Reset(RSIndexResult *r) {
  array_foreach(r->metrics, adtnl, RSValue_Decref(adtnl.value));
  array_clear(r->metrics);
}

/* Clear / free the metrics of a result */
void ResultMetrics_Free(RSIndexResult *r);

void Term_Offset_Data_Free(RSTermRecord *tr);

/* Reset the aggregate result's child vector */
static inline void AggregateResult_Reset(RSIndexResult *r) {

  r->docId = 0;
  r->data.agg.numChildren = 0;
  r->data.agg.typeMask = (RSResultType)0;
  ResultMetrics_Free(r);
}

/* Create a deep copy of the results that is totally thread safe. This is very slow so use it with
 * caution */
RSIndexResult *IndexResult_DeepCopy(const RSIndexResult *res);

/* Get the minimal delta between the terms in the result */
int IndexResult_MinOffsetDelta(const RSIndexResult *r);

/* Fill an array of max capacity cap with all the matching text terms for the result. The number of
 * matching terms is returned */
size_t IndexResult_GetMatchedTerms(RSIndexResult *r, RSQueryTerm **arr, size_t cap);

/* Return 1 if the the result is within a given slop range, inOrder determines whether the tokens
 * need to be ordered as in the query or not */
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

#ifdef __cplusplus
}
#endif
#endif
