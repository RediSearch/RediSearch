/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(RSIndexResult *h);

static inline void ResultMetrics_Concat(RSIndexResult *parent, RSIndexResult *child) {
  if (child->metrics) {
    // Passing ownership over the RSValues in the child metrics, but not on the array itself
    parent->metrics = array_ensure_append_n(parent->metrics, child->metrics, array_len(child->metrics));
    array_clear(child->metrics);
  }
}

static inline void ResultMetrics_Add(RSIndexResult *r, RLookupKey *key, RSValue *val) {
  RSYieldableMetric new_element = {.key = key, .value = val};
  r->metrics = array_ensure_append_1(r->metrics, new_element);
}

static inline void ResultMetrics_Reset(RSIndexResult *r) {
  array_foreach(r->metrics, adtnl, RSValue_Decref(adtnl.value));
  array_clear(r->metrics);
}

static inline void ResultMetrics_Free(RSIndexResult *r) {
  array_free_ex(r->metrics, RSValue_Decref(((RSYieldableMetric *)ptr)->value));
  r->metrics = NULL;
}

/* Prepare an Index Result to be reused. Add here any relevant cleanup function */
static inline void IndexResult_Clear(RSIndexResult *r) {
  ResultMetrics_Free(r);
}

/* Reset the aggregate result's child vector */
static inline void AggregateResult_Reset(RSIndexResult *r) {

  r->docId = 0;
  r->agg.numChildren = 0;
  r->agg.typeMask = (RSResultType)0;
  IndexResult_Clear(r);
}
/* Allocate a new intersection result with a given capacity*/
RSIndexResult *NewIntersectResult(size_t cap, double weight);

/* Allocate a new union result with a given capacity*/
RSIndexResult *NewUnionResult(size_t cap, double weight);

RSIndexResult *NewVirtualResult(double weight);

RSIndexResult *NewNumericResult();

RSIndexResult *NewMetricResult();

RSIndexResult *NewHybridResult();

/* Allocate a new token record result for a given term */
RSIndexResult *NewTokenRecord(RSQueryTerm *term, double weight);

/* Append a child to an aggregate result */
static inline void AggregateResult_AddChild(RSIndexResult *parent, RSIndexResult *child) {

  RSAggregateResult *agg = &parent->agg;

  /* Increase capacity if needed */
  if (agg->numChildren >= agg->childrenCap) {
    agg->childrenCap = agg->childrenCap ? agg->childrenCap * 2 : 1;
    agg->children = (__typeof__(agg->children))rm_realloc(
        agg->children, agg->childrenCap * sizeof(RSIndexResult *));
  }
  agg->children[agg->numChildren++] = child;
  // update the parent's type mask
  agg->typeMask |= child->type;
  parent->freq += child->freq;
  parent->docId = child->docId;
  parent->fieldMask |= child->fieldMask;
  ResultMetrics_Concat(parent, child);
}
/* Create a deep copy of the results that is totally thread safe. This is very slow so use it with
 * caution */
RSIndexResult *IndexResult_DeepCopy(const RSIndexResult *res);

/* Debug print a result */
void IndexResult_Print(RSIndexResult *r, int depth);

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
