/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#pragma once

#include "iterator_api.h"
#include "util/timeout.h"
#include "query_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;
  t_docId topId;
  t_docId currentId;
  t_docId numDocs;
} WildcardIterator, WildCardIteratorCtx;

/**
 * @param maxId - The maxID to return
 * @param numDocs - the number of docs to return
 */
QueryIterator *IT_V2(NewWildcardIterator_NonOptimized)(t_docId maxId, size_t numDocs);

#ifdef __cplusplus
}
#endif
