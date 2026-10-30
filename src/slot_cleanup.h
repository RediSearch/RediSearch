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

// Upper bound on DMDs visited per read-lock pass. A pass always finishes the bucket it is in, so a
// long chain can overshoot it.
#define SLOT_CLEANUP_SCAN_LIMIT 4096
// Upper bound on docs collected, then deleted under one write lock, per run of a cleanup task. With
// SLOT_CLEANUP_SCAN_LIMIT it bounds the collected-id buffer and how long a run holds the clean pool
// before requeueing itself.
#define SLOT_CLEANUP_CHUNK_DOC_LIMIT 512

// Drop every document of `slots` from all in-memory indexes. The docs disappear from queries
// before this returns; the memory is reclaimed by one background task per index on the clean pool.
// No-op in disk mode. Main thread only: it walks the global spec registry.
void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots);

// Phases of a cleanup task run, exposed for tests. Neither keeps DocTable pointers across lock
// releases, so concurrent writes between passes are safe.

// Where the collect phase resumes. Buckets are scanned in order; the bound is captured on the first
// pass, since buckets the table grows later only hold docs newer than the deletion.
typedef struct {
  bool started;
  size_t nextBucket;
  size_t bucketLimit;
} SlotCleanupCursor;

static inline bool SlotCleanup_CursorDone(const SlotCleanupCursor *cursor) {
  return cursor->started && cursor->nextBucket >= cursor->bucketLimit;
}

// The ids of the next docs `pending` covers, gathered in one bounded read-lock pass from `cursor`.
// May be empty while the cursor is not done, when the pass found no covered docs.
arrayof(t_docId) SlotCleanup_CollectDocIds(IndexSpec *spec, const PendingSlotDeletion *pending,
                                           SlotCleanupCursor *cursor);

// Delete `docIds` under the write lock, skipping ids that are already gone, and defer the GC
// notification to the GC's next run.
void SlotCleanup_ApplyDeletes(IndexSpec *spec, arrayof(t_docId) docIds);

#ifdef __cplusplus
}
#endif
#endif  // SLOT_CLEANUP_H__
