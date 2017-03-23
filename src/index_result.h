#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "redisearch.h"

#define DEFAULT_RECORDLIST_SIZE 4

RSQueryTerm *NewTerm(RSToken *tok);
void Term_Free(RSQueryTerm *t);

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(RSIndexResult *h);

void AggregateResult_Reset(RSAggregateResult *r);
/** Init a new index hit. This is not a heap allocation and doesn't neeed to be
 * freed */
RSIndexResult *NewIndexResult();
RSIndexResult *NewIntersectResult(size_t cap);
RSIndexResult *NewUnionResult(size_t cap);
RSIndexResult *NewTokenRecord(RSQueryTerm *term);

void AggregateResult_AddChild(RSIndexResult *parent, RSIndexResult *child);
void IndexResult_Print(RSIndexResult *r, int depth);
void IndexResult_Free(RSIndexResult *r);

int IndexResult_MinOffsetDelta(RSIndexResult *r);
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

#endif