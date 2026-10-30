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

// Scan budget per read-lock pass: each DMD or empty bucket consumes one unit.
// A pass finishes its current bucket, so a long chain can overshoot the budget.
#define SLOT_CLEANUP_SCAN_LIMIT 4096
// Upper bound on docs collected, then deleted under one write lock, per run of a cleanup task. With
// SLOT_CLEANUP_SCAN_LIMIT it bounds the collected-id buffer and how long a run holds the clean pool
// before requeueing itself.
#define SLOT_CLEANUP_CHUNK_DOC_LIMIT 512

// Hide documents in `slots` immediately and schedule bounded cleanup on the clean pool.
// Main thread only: it walks the global spec registry.
void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots);

// SST/RDB captures retain notifications the backend has not accepted yet.
// Called under the spec lock on save, and before publishing the spec on load.
void SlotCleanup_RdbSaveUnregistered(RedisModuleIO *rdb, const IndexSpec *spec);
bool SlotCleanup_RdbLoadUnregistered(RedisModuleIO *rdb, IndexSpec *spec);

// Module startup, before disk jobs can be scheduled; requires previously stopped workers drained.
void SlotCleanup_StartDiskJobs(void);
// Main-thread cron retries jobs retained after queue rejection or storage failure.
void SlotCleanup_RetryDiskJobs(void);
// Capture boundary: drain RAM-only batch apply, then reject new batches until resume.
// Slot-CF scans do not hold this gate and cannot delay a capture.
void SlotCleanup_PauseDiskJobs(void);
void SlotCleanup_ResumeDiskJobs(void);
// Shutdown: prevent new backend access and drain in-flight batches before closing disk handles.
void SlotCleanup_StopDiskJobs(void);

#ifdef ENABLE_ASSERT
// Test-only: reject the next disk-cleanup submissions through the queue failure path.
void SlotCleanup_FailNextDiskEnqueues(size_t count);
#endif

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
