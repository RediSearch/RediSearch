#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"
#define DEFAULT_RECORDLIST_SIZE 4

#ifdef __cplusplus
extern "C" {
#endif

RSQueryTerm *NewQueryTerm(RSToken *tok, int id);
void Term_Free(RSQueryTerm *t);

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(RSIndexResult *h);

/* Reset the aggregate result's child vector */
static inline void AggregateResult_Reset(RSIndexResult *r) {

  r->docId = 0;
  r->agg.numChildren = 0;
  r->agg.typeMask = (RSResultType)0;
}
/* Allocate a new intersection result with a given capacity*/
RSIndexResult *NewIntersectResult(size_t cap, double weight);

/* Allocate a new union result with a given capacity*/
RSIndexResult *NewUnionResult(size_t cap, double weight);

RSIndexResult *NewVirtualResult(double weight);

RSIndexResult *NewNumericResult();

RSIndexResult *NewDistanceResult();

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
}
/* Create a deep copy of the results that is totall thread safe. This is very slow so use it with
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