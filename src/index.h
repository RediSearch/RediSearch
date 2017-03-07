#ifndef __INDEX_H__
#define __INDEX_H__

#include "doc_table.h"
#include "forward_index.h"
#include "index_result.h"
#include "index_iterator.h"
#include "redisearch.h"
#include "util/logging.h"
#include "varint.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Free the internal data of an index hit. Since index hits are usually on the
stack,
this does not actually free the hit itself */
void IndexResult_Terminate(RSIndexResult *h);

/** Load document metadata for an index hit, marking it as having metadata.
Currently has no effect due to performance issues */
int IndexResult_LoadMetadata(RSIndexResult *h, DocTable *dt);

/* Free a union iterator */
void UnionIterator_Free(IndexIterator *it);

/* Free an intersect iterator */
void IntersectIterator_Free(IndexIterator *it);

/* Free a read iterator */
void ReadIterator_Free(IndexIterator *it);

/* UnionContext is used during the running of a union iterator */
typedef struct {
  IndexIterator **its;
  int num;
  int pos;
  size_t len;
  t_docId minDocId;
  RSIndexResult *currentHits;
  DocTable *docTable;
  int atEnd;
} UnionContext;

/* Create a new UnionIterator over a list of underlying child iterators.
It will return each document of the underlying iterators, exactly once */
IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *t);

int UI_SkipTo(void *ctx, u_int32_t docId, RSIndexResult *hit);
int UI_Next(void *ctx);
int UI_Read(void *ctx, RSIndexResult *hit);
int UI_HasNext(void *ctx);
size_t UI_Len(void *ctx);
t_docId UI_LastDocId(void *ctx);

/* The context used by the intersection methods during iterating an intersect
 * iterator */
typedef struct {
  IndexIterator **its;
  int num;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId;
  RSIndexResult *currentHits;
  DocTable *docTable;
  uint32_t fieldMask;
  int atEnd;
} IntersectContext;

/* Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
 * negative number, we will allow at most maxSlop intervening positions between the terms. If
 * maxSlop is set and inOrder is 1, we assert that the terms are in
 * order. I.e anexact match has maxSlop of 0 and inOrder 1.  */
IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *t, u_char fieldMask,
                                   int maxSlop, int inOrder);

int II_SkipTo(void *ctx, u_int32_t docId, RSIndexResult *hit);
int II_Next(void *ctx);
int II_Read(void *ctx, RSIndexResult *hit);
int II_HasNext(void *ctx);
size_t II_Len(void *ctx);
t_docId II_LastDocId(void *ctx);

#endif