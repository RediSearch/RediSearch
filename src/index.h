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
  t_docId *docIds;
  int num;
  int pos;
  size_t len;
  t_docId minDocId;
  RSIndexResult *current;
  DocTable *docTable;
  int atEnd;
  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
} UnionContext;

/* Create a new UnionIterator over a list of underlying child iterators.
It will return each document of the underlying iterators, exactly once */
IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *t, int quickExit);
RSIndexResult *UI_Current(void *ctx);
int UI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);
int UI_Next(void *ctx);
int UI_Read(void *ctx, RSIndexResult **hit);
int UI_HasNext(void *ctx);
size_t UI_Len(void *ctx);
t_docId UI_LastDocId(void *ctx);

/* The context used by the intersection methods during iterating an intersect
 * iterator */
typedef struct {
  IndexIterator **its;
  t_docId *docIds;
  int *rcs;
  RSIndexResult *current;
  int num;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId;

  // RSIndexResult *result;
  DocTable *docTable;
  t_fieldMask fieldMask;
  int atEnd;
} IntersectContext;

/* Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
 * negative number, we will allow at most maxSlop intervening positions between the terms. If
 * maxSlop is set and inOrder is 1, we assert that the terms are in
 * order. I.e anexact match has maxSlop of 0 and inOrder 1.  */
IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *t, t_fieldMask fieldMask,
                                   int maxSlop, int inOrder);

int II_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);
int II_Next(void *ctx);
int II_Read(void *ctx, RSIndexResult **hit);
int II_HasNext(void *ctx);
RSIndexResult *II_Current(void *ctx);
size_t II_Len(void *ctx);
t_docId II_LastDocId(void *ctx);

/* A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for
 * hits */
typedef struct {
  IndexIterator *child;
  RSIndexResult *current;
  t_docId lastDocId;
  t_docId maxDocId;
  size_t len;
} NotContext;

/* Create an Optional clause iterator by wrapping another index iterator. An optional iterator
 * always returns OK on skips, but a virtual hit with frequency of 0 if there is no hit */
IndexIterator *NewNotIterator(IndexIterator *it, t_docId maxDocId);

typedef struct {
  IndexIterator *child;
  RSIndexResult *virt;
  RSIndexResult *current;
  t_fieldMask fieldMask;
  t_docId lastDocId;
} OptionalMatchContext;

/* Create a NOT iterator by wrapping another index iterator */
IndexIterator *NewOptionalIterator(IndexIterator *it);

/* Create a wildcard iterator, matching ALL documents in the index. This is used for one thing only
 * -
 * purely negative queries. If the root of the query is a negative expression, we cannot process it
 * without a positive expression. So we create a wildcard iterator that basically just iterates all
 * the incremental document ids, and matches every skip within its range. */
IndexIterator *NewWildcardIterator(t_docId maxId);

#endif