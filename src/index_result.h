#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "redisearch.h"

#define DEFAULT_RECORDLIST_SIZE 4

RSQueryTerm *NewQueryTerm(RSToken *tok, int id);
void Term_Free(RSQueryTerm *t);

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(RSIndexResult *h);

void AggregateResult_Reset(RSIndexResult *r);

/* Allocate a new intersection result with a given capacity*/
RSIndexResult *NewIntersectResult(size_t cap);

/* Allocate a new union result with a given capacity*/
RSIndexResult *NewUnionResult(size_t cap);

RSIndexResult *NewVirtualResult();

RSIndexResult *NewNumericResult();

/* Allocate a new token record result for a given term */
RSIndexResult *NewTokenRecord(RSQueryTerm *term);

/* Append a child to an aggregate result */
void AggregateResult_AddChild(RSIndexResult *parent, RSIndexResult *child);

/* Create a deep copy of the results that is totall thread safe. This is very slow so use it with
 * caution */
RSIndexResult *IndexResult_DeepCopy(const RSIndexResult *res);

/* Debug print a result */
void IndexResult_Print(RSIndexResult *r, int depth);

/* Free an index result's internal allocations, does not free the result itself */
void IndexResult_Free(RSIndexResult *r);

/* Get the minimal delta between the terms in the result */
int IndexResult_MinOffsetDelta(RSIndexResult *r);

/* Return 1 if the the result is within a given slop range, inOrder determines whether the tokens
 * need to be ordered as in the query or not */
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

#endif