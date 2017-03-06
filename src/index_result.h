#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "types.h"
#include "varint.h"
#include "redisearch.h"

#define DEFAULT_RECORDLIST_SIZE 4

RSQueryTerm *NewTerm(char *str);
void Term_Free(RSQueryTerm *t);

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(RSIndexResult *h);
/** Init a new index hit. This is not a heap allocation and doesn't neeed to be
 * freed */
RSIndexResult NewIndexResult();

void IndexResult_PutRecord(RSIndexResult *r, RSIndexRecord *record);
void IndexResult_Add(RSIndexResult *dst, RSIndexResult *src);
void IndexResult_Print(RSIndexResult *r);
void IndexResult_Free(RSIndexResult *r);
int IndexResult_MinOffsetDelta(RSIndexResult *r);
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

#endif