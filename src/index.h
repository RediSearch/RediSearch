/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEX_H__
#define __INDEX_H__

#include "doc_table.h"
#include "forward_index.h"
#include "index_result.h"
#include "iterators/iterator_api.h"
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

/* Add an iterator to an intersect iterator */
void AddIntersectIterator(IndexIterator *parentIter, IndexIterator *childIter);

/* Trim a union iterator to hold minimum iterators that contain `limit` results.
 * This is used to optimize queries with no additional filters. */
void trimUnionIterator(IndexIterator *iter, size_t offset, size_t limit, bool asc);

#ifdef __cplusplus
}
#endif

#endif // __INDEX_H__
