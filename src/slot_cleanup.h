/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SLOT_CLEANUP_H__
#define SLOT_CLEANUP_H__

#include "redismodule.h"
#include "spec.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

// Upper bound on DMDs examined per read-lock pass of the collect phase. A pass always finishes
// the bucket it is in, so a long chain can overshoot it.
#define SLOT_CLEANUP_SCAN_DOC_LIMIT 4096
// Upper bound on docs deleted per write-lock pass of the apply phase.
#define SLOT_CLEANUP_APPLY_DOC_LIMIT 512

// Drop every document of `slots` from all in-memory indexes. The docs disappear from queries
// before this returns; the memory is reclaimed by one background task per index on the clean pool.
// No-op in disk mode. Main thread only: it walks the global spec registry.
void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots);

// Phases of the per-index background task, exposed for tests. Neither keeps DocTable pointers
// across lock releases, so concurrent writes between passes are safe.

// The ids of the docs `pending` covers, gathered under bounded read-lock passes.
arrayof(t_docId) SlotCleanup_CollectDocIds(IndexSpec *spec, const PendingSlotDeletion *pending);

// Delete `docIds` under bounded write-lock passes. Ids that are already gone are skipped.
void SlotCleanup_ApplyDeletes(IndexSpec *spec, arrayof(t_docId) docIds);

#ifdef __cplusplus
}
#endif
#endif  // SLOT_CLEANUP_H__
