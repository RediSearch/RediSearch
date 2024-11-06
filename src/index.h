/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __INDEX_H__
#define __INDEX_H__

#include "doc_table.h"
#include "forward_index.h"
#include "index_result.h"
#include "index_iterator.h"
#include "inverted_index.h"
#include "redisearch.h"
#include "varint.h"
#include "query_node.h"
#include "reply.h"
#include "query_ctx.h"

#include "util/logging.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Free the internal data of an index hit. Since index hits are usually on the
stack, this does not actually free the hit itself */
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

/* Create a new UnionIterator over a list of underlying child iterators.
It will return each document of the underlying iterators, exactly once */
IndexIterator *NewUnionIterator(IndexIterator **its, int num, int quickExit,
                                double weight, QueryNodeType type, const char *qstr, IteratorsConfig *config);

void UI_Foreach(IndexIterator *it, void (*callback)(IndexReader *it));

/* Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
 * negative number, we will allow at most maxSlop intervening positions between the terms. If
 * maxSlop is set and inOrder is 1, we assert that the terms are in
 * order. I.e anexact match has maxSlop of 0 and inOrder 1.  */
IndexIterator *NewIntersectIterator(IndexIterator **its, size_t num, DocTable *t,
                                   t_fieldMask fieldMask, int maxSlop, int inOrder, double weight);

/* Add an iterator to an intersect iterator */
void AddIntersectIterator(IndexIterator *parentIter, IndexIterator *childIter);

/* Trim a union iterator to hold minimum iterators that contain `limit` results.
 * This is used to optimize queries with no additional filters. */
void trimUnionIterator(IndexIterator *iter, size_t offset, size_t limit, bool asc);

/* Create a NOT iterator by wrapping another index iterator */
IndexIterator *NewNotIterator(IndexIterator *it, t_docId maxDocId,
  double weight, struct timespec timeout, QueryEvalCtx *q);

/* Create an Optional clause iterator by wrapping another index iterator. An optional iterator
 * always returns OK on skips, but a virtual hit with frequency of 0 if there is no hit */
IndexIterator *NewOptionalIterator(IndexIterator *it, t_docId maxDocId, double weight);

/* Create a wildcard iterator, to iterate all the existing docs in the*/
IndexIterator *NewWildcardIterator(QueryEvalCtx *q);

/* Create a new IdListIterator from a pre populated list of document ids of size num. The doc ids
 * are sorted in this function, so there is no need to sort them. They are automatically freed in
 * the end and assumed to be allocated using rm_malloc */
IndexIterator *NewIdListIterator(t_docId *ids, t_offset num, double weight);

/** Create a new iterator which returns no results */
IndexIterator *NewEmptyIterator(void);

/** Add Profile iterator layer between iterators */
void Profile_AddIters(IndexIterator **root);

typedef struct {
    IteratorsConfig *iteratorsConfig;
    int printProfileClock;
} PrintProfileConfig;

// Print profile of iterators
void printIteratorProfile(RedisModule_Reply *reply, IndexIterator *root, size_t counter,
                          double cpuTime, int depth, int limited, PrintProfileConfig *config);

#ifdef __cplusplus
}
#endif

#endif // __INDEX_H__
