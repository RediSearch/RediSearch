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
#include "query_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

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
* When `q->sctx` carries no deadline - including every `RS_IsMock` build, where `SearchCtx_UpdateTime`
* never arms one - the optimized iterator copies `timeout` and probes that instead, while the
* non-optimized (Rust) iterator skips timeout checks outright. Callers which cannot meet the two
* requirements above must hand over such a context; see `NI_LiveDeadline`.
*/
QueryIterator *NewNotIterator(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q);

// Constructor used for benchmarking (easy to inject MockIterators)
// timeoutCounter: initial counter value (use REDISEARCH_UNINITIALIZED to skip timeout checks)
// Takes no query context, so `timeout` is always copied into the iterator and probed from there.
QueryIterator *_New_NotIterator_With_WildCardIterator(QueryIterator *child, QueryIterator *wcii, t_docId maxDocId, double weight, struct timespec timeout, uint32_t timeoutCounter);

QueryIterator const *GetNotIteratorChild(const QueryIterator *const it);
void SetNotIteratorChild(QueryIterator *it, QueryIterator* child);
QueryIterator *TakeNotIteratorChild(QueryIterator *it);

// Setter used only for cpp unit tests of NOT iterator.
// Should be removed once we port+swap the optimized version of NOT iterator.
void _SetNotIteratorOptimizedWildcard(QueryIterator *it, QueryIterator* wcii);
// Getter used only for cpp unit tests of NOT iterator.
// Should be removed once we port+swap the optimized version of NOT iterator.
QueryIterator const *_GetNotIteratorOptimizedWildcard(const QueryIterator *it);

#ifdef __cplusplus
}
#endif
