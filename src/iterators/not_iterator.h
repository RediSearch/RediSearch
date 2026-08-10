/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "iterator_api.h"
#include "util/timeout.h"
#include "query_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;         // base index iterator
  QueryIterator *wcii;        // wildcard index iterator
  QueryIterator *child;       // child index iterator
  t_docId maxDocId;
  // Amortization counter for the deadline probe, and the deadline `deadline` falls back to when the
  // query has none of its own. Never probe `timeoutCtx.timeout` directly: go through `deadline`.
  TimeoutCtx timeoutCtx;
  // The deadline every read probes. Points either into the query's search context or at
  // `timeoutCtx.timeout`; which one, and what that costs the caller, is decided by
  // `NewNotIterator`.
  const struct timespec *deadline;
} NotIterator;

/**
* Create an iterator over every docId up to `maxDocId` that `it` does not contain.
*
* @param it - The iterator to negate
* @param maxDocId - the maximum docId
* @param weight - the weight of the node (assigned to the returned result)
* @param timeout - deadline to fall back on, used only when `q` carries none (see below)
* @param q - the query context. Must not be NULL.
*
* Reads probe a deadline once every few thousand skipped documents. When `q->sctx` carries a
* deadline, that deadline is probed *by reference* rather than copied, so a cursor read that re-arms
* it in place is measured against its own budget instead of an earlier read's (MOD-17489). Probing by
* reference asks two things of the caller:
*
*   - `q->sctx` must stay alive at a stable address for as long as the returned iterator is used,
*     not merely for the duration of this call;
*   - no write to `q->sctx->time.timeout` may overlap a read of the returned iterator.
*
* A request satisfies both: `AREQ_ApplyContext` assigns `req->sctx` once, and cursor reads re-arm the
* deadline between reads, never during one.
*
* A `q->sctx` with no deadline set gets `timeout` copied into the iterator and probed instead. That
* is the escape hatch for callers which cannot meet the two requirements above - benchmarks handing
* over a throwaway context - and it mirrors the deadline-less case on `master`, where the iterator
* opts out of timeout checks altogether.
*/
QueryIterator *NewNotIterator(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q);

// Constructor used for benchmarking (easy to inject MockIterators).
// Takes no query context, so `timeout` is always copied into the iterator and probed from there.
QueryIterator *_New_NotIterator_With_WildCardIterator(QueryIterator *child, QueryIterator *wcii, t_docId maxDocId, double weight, struct timespec timeout);

#ifdef __cplusplus
}
#endif
