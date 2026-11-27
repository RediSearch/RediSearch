/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef INDEX_LOADING_H__
#define INDEX_LOADING_H__

#include <stdbool.h>

#include "query_error.h"
#include "query_node.h"

/* Index loading: the `$as` query attribute names the value a NUMERIC clause
 * matched, so the pipeline reads it as a field of each result instead of
 * loading it from the document. An unstable feature, gated on
 * `search-enable-unstable-features`.
 *
 * A loading node carries `QueryNodeFlags_LoadsValue` and stores the alias on its
 * `opts.distField`; query evaluation reserves a yield request under it and wraps
 * the clause's iterator so each hit carries the value. */

#define AS_ATTR "as"

/* Apply `$as` to `qn`, taking ownership of the attribute's value on success.
 * Returns 1 on success, 0 with `status` set otherwise. */
int IndexLoading_ApplyAlias(QueryNode *qn, QueryAttribute *attr, QueryError *status);

/* Whether `qn` loads a value through `$as`. */
static inline bool IndexLoading_NodeLoads(const QueryNode *qn) {
  return qn->opts.flags & QueryNodeFlags_LoadsValue;
}

#endif  // INDEX_LOADING_H__
