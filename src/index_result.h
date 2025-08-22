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

/* Add the metrics of a child to a parent. */
void RSYieldableMetric_Concat(RSYieldableMetric **parent, RSYieldableMetric *child);

/* Clear / free the metrics of a result */
void ResultMetrics_Free(RSIndexResult *r);

/* Make a complete clone of the metrics array and increment the reference count of each value  */
RSYieldableMetric* RSYieldableMetrics_Clone(RSYieldableMetric *src);

static inline void ResultMetrics_Add(RSIndexResult *r, RLookupKey *key, RSValue *val) {
  RSYieldableMetric new_element = {.key = key, .value = val};
  r->metrics = array_ensure_append_1(r->metrics, new_element);
}

static inline void ResultMetrics_Reset(RSIndexResult *r) {
  array_foreach(r->metrics, adtnl, RSValue_Decref(adtnl.value));
  array_clear(r->metrics);
}

/* Reset the aggregate result's child vector */
static inline void IndexResult_ResetAggregate(RSIndexResult *r) {

  r->docId = 0;
  r->freq = 0;
  r->fieldMask = 0;
  IndexResult_AggregateReset(r);
  ResultMetrics_Free(r);
}
/* Allocate a new intersection result with a given capacity*/
RSIndexResult *NewIntersectResult(size_t cap, double weight);

/* Allocate a new union result with a given capacity*/
RSIndexResult *NewUnionResult(size_t cap, double weight);

RSIndexResult *NewVirtualResult(double weight, t_fieldMask fieldMask);

RSIndexResult *NewNumericResult();

RSIndexResult *NewMetricResult();

RSIndexResult *NewHybridResult();

/* Allocate a new token record result for a given term */
RSIndexResult *NewTokenRecord(RSQueryTerm *term, double weight);

/* Create a deep copy of the results that is totally thread safe. This is very slow so use it with
 * caution */
RSIndexResult *IndexResult_DeepCopy(const RSIndexResult *res);

/* Free an index result's internal allocations, does not free the result itself */
void IndexResult_Free(RSIndexResult *r);

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
