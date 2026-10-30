/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "slot_cleanup.h"

#include "indexes.h"
#include "rmalloc.h"
#include "search_disk.h"
#include "slot_ranges.h"

#include <pthread.h>
#include <stdatomic.h>
#include <limits.h>

typedef struct {
  WeakRef spec_ref;
  // Owned by the spec's DocTable, which frees it if the spec is dropped first; only touched
  // after the weak ref was promoted.
  PendingSlotDeletion *pending;
  SlotCleanupCursor cursor;
} SlotCleanupCtx;

// TODO: consider a single pass under the write lock that unchains matching DMDs in place. It saves
// the id buffer and the second lookup per doc, but holds the exclusive lock while scanning docs it
// will not delete, which costs queries when only a small share of the docs is in deleted slots.
arrayof(t_docId) SlotCleanup_CollectDocIds(IndexSpec *spec, const PendingSlotDeletion *pending,
                                           SlotCleanupCursor *cursor) {
  // Sized for a full chunk; a pass that finishes a long chain may still grow it.
  arrayof(t_docId) docIds = array_new(t_docId, SLOT_CLEANUP_CHUNK_DOC_LIMIT);

  IndexSpec_LockRead(spec);
  const DocTable *t = &spec->docs;
  // Every doc the deletion covers already existed when it was announced, so it sits in a bucket
  // below the capacity seen now.
  if (!cursor->started) {
    cursor->started = true;
    cursor->bucketLimit = t->cap;
  }
  size_t visited = 0;
  while (cursor->nextBucket < cursor->bucketLimit && visited < SLOT_CLEANUP_SCAN_LIMIT &&
         array_len(docIds) < SLOT_CLEANUP_CHUNK_DOC_LIMIT) {
    const RSDocumentMetadata *root = t->buckets[cursor->nextBucket].root;
    // Empty buckets still cost a lookup even when few documents remain in a large table.
    if (!root) {
      ++visited;
    }
    for (const RSDocumentMetadata *dmd = root; dmd; dmd = dmd->nextInChain) {
      ++visited;
      if (PendingSlotDeletion_Covers(pending, dmd)) {
        array_append(docIds, dmd->id);
      }
    }
    ++cursor->nextBucket;
  }
  IndexSpec_Unlock(spec);
  return docIds;
}

void SlotCleanup_ApplyDeletes(IndexSpec *spec, arrayof(t_docId) docIds) {
  if (array_len(docIds) == 0) {
    return;
  }
  size_t deleted = 0;
  IndexSpec_IncrActiveWrites(spec);
  IndexSpec_LockWrite(spec);
  for (size_t i = 0; i < array_len(docIds); ++i) {
    deleted += IndexSpec_DeleteDocById_Unsafe(spec, docIds[i]);
  }
  IndexSpec_DecrActiveWrites(spec);
  IndexSpec_Unlock(spec);
  IndexSpec_DeferGCDeletes(spec, deleted);
}

// Runs one chunk; returns whether the cleanup is complete.
static bool runChunk(IndexSpec *spec, SlotCleanupCtx *ctx) {
  arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, ctx->pending, &ctx->cursor);
  SlotCleanup_ApplyDeletes(spec, docIds);
  array_free(docIds);
  if (!SlotCleanup_CursorDone(&ctx->cursor)) {
    return false;
  }
  IndexSpec_LockWrite(spec);
  DocTable_RemovePendingDeletion(&spec->docs, ctx->pending);
  IndexSpec_Unlock(spec);
  return true;
}

static void SlotCleanup_Run(void *arg) {
  SlotCleanupCtx *ctx = arg;
  for (;;) {
    StrongRef spec_ref = WeakRef_Promote(ctx->spec_ref);
    IndexSpec *spec = StrongRef_Get(spec_ref);
    if (!spec) {
      break;
    }
    bool done = runChunk(spec, ctx);
    StrongRef_Release(spec_ref);
    if (done) {
      break;
    }
    // Requeue between chunks, so jobs queued meanwhile (index teardowns, other indexes' cleanups)
    // wait for one chunk rather than the whole cleanup. Keep going here if the queue rejects it.
    if (CleanPool_AddWork(SlotCleanup_Run, ctx) == 0) {
      return;
    }
  }
  WeakRef_Release(ctx->spec_ref);
  rm_free(ctx);
}

static void scheduleSlotCleanup(IndexSpec *spec, void *slots) {
  IndexSpec_LockWrite(spec);
  // An empty index has nothing to hide, and a node would only slow lookups until its task runs.
  if (DocTable_IsEmpty(&spec->docs)) {
    IndexSpec_Unlock(spec);
    return;
  }
  PendingSlotDeletion *pending = DocTable_AddPendingDeletion(&spec->docs, slots);
  IndexSpec_Unlock(spec);

  SlotCleanupCtx *ctx = rm_new(SlotCleanupCtx);
  ctx->spec_ref = StrongRef_Demote(spec->own_ref);
  ctx->pending = pending;
  ctx->cursor = (SlotCleanupCursor){0};
  // Until the task ends, every DocTable_Borrow also checks the pending list, and the GC cannot
  // learn about these deletions, so finishing it matters more than reclaiming memory.
  if (CleanPool_AddWork(SlotCleanup_Run, ctx) != 0) {
    // Run it here rather than leave the docs hidden but never deleted. Later chunks still move to
    // the pool once it accepts work again.
    RedisModule_Log(RSDummyContext, "warning",
                    "Could not queue slot cleanup, running it on the main thread");
    SlotCleanup_Run(ctx);
  }
}

struct IndexSpecSlotTrimJob {
  WeakRef specRef;
  uint64_t trimId;
  PendingSlotDeletion *pending;
  bool finished;
  bool queued;
  struct IndexSpecSlotTrimJob *next;
};

// Only shutdown takes this lock exclusively; normal main-thread callbacks never wait for I/O.
static pthread_rwlock_t diskJobRunLock = PTHREAD_RWLOCK_INITIALIZER;
static atomic_bool diskJobsStopped = false;
static pthread_mutex_t diskApplyLock = PTHREAD_MUTEX_INITIALIZER;
static bool diskJobsPaused;
static pthread_mutex_t diskJobListLock = PTHREAD_MUTEX_INITIALIZER;
static IndexSpecSlotTrimJob *diskJobs;

void SlotCleanup_StartDiskJobs(void) {
  pthread_mutex_lock(&diskApplyLock);
  diskJobsPaused = false;
  pthread_mutex_unlock(&diskApplyLock);
  atomic_store(&diskJobsStopped, false);
}

void SlotCleanup_PauseDiskJobs(void) {
  pthread_mutex_lock(&diskApplyLock);
  diskJobsPaused = true;
  pthread_mutex_unlock(&diskApplyLock);
}

void SlotCleanup_ResumeDiskJobs(void) {
  pthread_mutex_lock(&diskApplyLock);
  diskJobsPaused = false;
  pthread_mutex_unlock(&diskApplyLock);
}

static PendingSlotDeletion *pendingDiskTrim(IndexSpec *spec, uint64_t trimId,
                                            const SearchDiskSlotRange *ranges, size_t numRanges,
                                            t_docId maxDocId) {
  for (PendingSlotDeletion *pending = spec->docs.pendingSlotDeletions; pending;
       pending = pending->next) {
    if (pending->diskTrimId == trimId) return pending;
  }
  RS_LOG_ASSERT_ALWAYS(numRanges <= INT32_MAX, "too many slot ranges");
  RedisModuleSlotRangeArray *slots = rm_malloc(SlotRangeArray_SizeOf(numRanges));
  slots->num_ranges = numRanges;
  for (size_t i = 0; i < numRanges; ++i) {
    slots->ranges[i].start = ranges[i].start;
    slots->ranges[i].end = ranges[i].end;
  }
  PendingSlotDeletion *pending =
      DocTable_AddPendingDeletionWithCutoff(&spec->docs, slots, maxDocId);
  pending->diskTrimId = trimId;
  rm_free(slots);
  return pending;
}

static IndexSpecSlotTrimJob *newDiskJob(StrongRef ref, uint64_t trimId,
                                        PendingSlotDeletion *pending) {
  IndexSpecSlotTrimJob *job = rm_calloc(1, sizeof(*job));
  job->specRef = StrongRef_Demote(ref);
  job->trimId = trimId;
  job->pending = pending;
  return job;
}

IndexSpecSlotTrimJob *IndexSpec_RegisterDiskSlotTrim(StrongRef specRef,
                                                     const SearchDiskSlotRange *ranges,
                                                     size_t numRanges) {
  IndexSpec *spec = StrongRef_Get(specRef);
  if (!spec || !spec->diskSpec || atomic_load(&diskJobsStopped)) return NULL;
  uint64_t trimId = 0;
  t_docId maxDocId = 0;
  IndexSpec_LockWrite(spec);
  bool registered =
      SearchDisk_RegisterSlotTrim(spec->diskSpec, ranges, numRanges, &trimId, &maxDocId);
  RS_LOG_ASSERT_ALWAYS(!registered || trimId != 0, "disk slot cleanup ID must be nonzero");
  PendingSlotDeletion *pending =
      registered ? pendingDiskTrim(spec, trimId, ranges, numRanges, maxDocId) : NULL;
  IndexSpec_Unlock(spec);
  return registered ? newDiskJob(specRef, trimId, pending) : NULL;
}

typedef struct {
  IndexSpec *spec;
  IndexSpecSlotTrimJob *job;
} DiskSlotTrimContext;

static bool trimMemoryIndexes(void *ctx, const t_docId *ids, size_t numIds, uint64_t totalDocLen,
                              const SearchDiskSlotTrimBatch *batch, bool exhausted) {
  DiskSlotTrimContext *trim = ctx;
  IndexSpec *spec = trim->spec;
  pthread_mutex_lock(&diskApplyLock);
  if (diskJobsPaused) {
    pthread_mutex_unlock(&diskApplyLock);
    return false;
  }
  IndexSpec_IncrActiveWrites(spec);
  IndexSpec_LockWrite(spec);
  bool applied = spec->stats.scoring.numDocuments >= numIds &&
                 spec->stats.scoring.totalDocsLen >= totalDocLen &&
                 SearchDisk_ApplySlotTrimBatch(spec->diskSpec, batch);
  if (applied) {
    for (size_t i = 0; i < numIds; ++i) IndexSpec_RemoveDocFromMemoryIndexes(spec, ids[i]);
    spec->stats.scoring.numDocuments -= numIds;
    spec->stats.scoring.totalDocsLen -= totalDocLen;
    IndexSpec_DeferGCDeletes(spec, numIds);
    if (exhausted) {
      DocTable_RemovePendingDeletion(&spec->docs, trim->job->pending);
      trim->job->pending = NULL;
      trim->job->finished = true;
    }
  }
  IndexSpec_Unlock(spec);
  IndexSpec_DecrActiveWrites(spec);
  pthread_mutex_unlock(&diskApplyLock);
  return applied;
}

IndexSpecDiskSlotTrimResult IndexSpec_RunDiskSlotTrim(IndexSpecSlotTrimJob *job) {
  if (job->finished) return INDEX_DISK_TRIM_FINISHED;
  pthread_rwlock_rdlock(&diskJobRunLock);
  if (atomic_load(&diskJobsStopped)) {
    pthread_rwlock_unlock(&diskJobRunLock);
    return INDEX_DISK_TRIM_CANCELLED;
  }
  StrongRef running = WeakRef_Promote(job->specRef);
  IndexSpec *spec = StrongRef_Get(running);
  IndexSpecDiskSlotTrimResult result = INDEX_DISK_TRIM_CANCELLED;
  pthread_mutex_lock(&diskApplyLock);
  bool paused = diskJobsPaused;
  pthread_mutex_unlock(&diskApplyLock);
  if (spec && spec->diskSpec && paused) {
    result = INDEX_DISK_TRIM_RETRY;
  } else if (spec && spec->diskSpec) {
    DiskSlotTrimContext context = {.spec = spec, .job = job};
    SearchDiskSlotTrimResult status = SearchDisk_RunSlotTrimBatch(
        spec->diskSpec, job->trimId, SLOT_CLEANUP_CHUNK_DOC_LIMIT, trimMemoryIndexes, &context);
    result = status == SEARCH_DISK_TRIM_FINISHED ? INDEX_DISK_TRIM_FINISHED
             : status == SEARCH_DISK_TRIM_MORE   ? INDEX_DISK_TRIM_MORE
                                                 : INDEX_DISK_TRIM_RETRY;
    RS_LOG_ASSERT_ALWAYS((result == INDEX_DISK_TRIM_FINISHED) == job->finished,
                         "slot cleanup completion must follow the final applied batch");
  }
  if (running.rm) StrongRef_Release(running);
  pthread_rwlock_unlock(&diskJobRunLock);
  return result;
}

void IndexSpec_FreeDiskSlotTrim(IndexSpecSlotTrimJob *job) {
  if (!job) return;
  WeakRef_Release(job->specRef);
  rm_free(job);
}

static void DiskSlotCleanup_Run(void *arg);

#ifdef ENABLE_ASSERT
static atomic_size_t diskQueueFailures;

void SlotCleanup_FailNextDiskEnqueues(size_t count) {
  atomic_store(&diskQueueFailures, count);
}
#endif

static int submitDiskJob(IndexSpecSlotTrimJob *job) {
#ifdef ENABLE_ASSERT
  size_t failures = atomic_load(&diskQueueFailures);
  while (failures != 0) {
    if (atomic_compare_exchange_weak(&diskQueueFailures, &failures, failures - 1)) return -1;
  }
#endif
  return CleanPool_AddWork(DiskSlotCleanup_Run, job);
}

// The list lock protects scheduler ownership only, never backend reads or spec locks.
static bool queueDiskJob(IndexSpecSlotTrimJob *job) {
  if (job->queued || atomic_load(&diskJobsStopped)) return false;
  job->queued = true;
  if (submitDiskJob(job) == 0) return true;
  job->queued = false;
  return false;
}

static void unlinkDiskJob(IndexSpecSlotTrimJob *job) {
  IndexSpecSlotTrimJob **link = &diskJobs;
  while (*link != job) {
    RS_LOG_ASSERT_ALWAYS(*link, "slot cleanup job missing from scheduler");
    link = &(*link)->next;
  }
  *link = job->next;
}

static void DiskSlotCleanup_Run(void *arg) {
  IndexSpecSlotTrimJob *job = arg;
  IndexSpecDiskSlotTrimResult result = IndexSpec_RunDiskSlotTrim(job);
  pthread_mutex_lock(&diskJobListLock);
  job->queued = false;
  if (result == INDEX_DISK_TRIM_FINISHED || result == INDEX_DISK_TRIM_CANCELLED ||
      atomic_load(&diskJobsStopped)) {
    unlinkDiskJob(job);
    pthread_mutex_unlock(&diskJobListLock);
    IndexSpec_FreeDiskSlotTrim(job);
    return;
  }
  if (result == INDEX_DISK_TRIM_MORE) queueDiskJob(job);
  // Retry failures on a later cron tick rather than spin on persistent storage errors.
  pthread_mutex_unlock(&diskJobListLock);
}

static void scheduleDiskJob(IndexSpecSlotTrimJob *job) {
  pthread_mutex_lock(&diskJobListLock);
  job->next = diskJobs;
  diskJobs = job;
  queueDiskJob(job);
  pthread_mutex_unlock(&diskJobListLock);
}

void SlotCleanup_RetryDiskJobs(void) {
  pthread_mutex_lock(&diskApplyLock);
  bool paused = diskJobsPaused;
  pthread_mutex_unlock(&diskApplyLock);
  if (paused || atomic_load(&diskJobsStopped)) return;
  pthread_mutex_lock(&diskJobListLock);
  for (IndexSpecSlotTrimJob *job = diskJobs; job; job = job->next) queueDiskJob(job);
  pthread_mutex_unlock(&diskJobListLock);
}

void SlotCleanup_StopDiskJobs(void) {
  atomic_store(&diskJobsStopped, true);
  pthread_rwlock_wrlock(&diskJobRunLock);
  pthread_rwlock_unlock(&diskJobRunLock);
  pthread_mutex_lock(&diskJobListLock);
  IndexSpecSlotTrimJob **link = &diskJobs;
  while (*link) {
    IndexSpecSlotTrimJob *job = *link;
    if (job->queued) {
      link = &job->next;
    } else {
      *link = job->next;
      IndexSpec_FreeDiskSlotTrim(job);
    }
  }
  pthread_mutex_unlock(&diskJobListLock);
}

static void resumeDiskTrim(void *ctx, uint64_t trimId, const SearchDiskSlotRange *ranges,
                           size_t numRanges, t_docId maxDocId) {
  IndexSpec *spec = ctx;
  RS_LOG_ASSERT_ALWAYS(trimId != 0, "disk slot cleanup ID must be nonzero");
  pthread_mutex_lock(&diskJobListLock);
  for (IndexSpecSlotTrimJob *job = diskJobs; job; job = job->next) {
    if (job->specRef.rm == spec->own_ref.rm && job->trimId == trimId) {
      pthread_mutex_unlock(&diskJobListLock);
      return;
    }
  }
  PendingSlotDeletion *pending = pendingDiskTrim(spec, trimId, ranges, numRanges, maxDocId);
  IndexSpecSlotTrimJob *job = newDiskJob(spec->own_ref, trimId, pending);
  job->next = diskJobs;
  diskJobs = job;
  queueDiskJob(job);
  pthread_mutex_unlock(&diskJobListLock);
}

void IndexSpec_ResumeDiskSlotTrims(IndexSpec *spec) {
  if (!spec->diskSpec || atomic_load(&diskJobsStopped)) return;
  IndexSpec_LockWrite(spec);
  SearchDisk_ForEachPendingSlotTrim(spec->diskSpec, resumeDiskTrim, spec);
  IndexSpec_Unlock(spec);
}

static void scheduleDiskSlotCleanup(IndexSpec *spec, void *arg) {
  const RedisModuleSlotRangeArray *slots = arg;
  SearchDiskSlotRange *ranges = rm_malloc(sizeof(*ranges) * slots->num_ranges);
  for (int32_t i = 0; i < slots->num_ranges; ++i) {
    ranges[i].start = slots->ranges[i].start;
    ranges[i].end = slots->ranges[i].end;
  }
  IndexSpecSlotTrimJob *job =
      IndexSpec_RegisterDiskSlotTrim(spec->own_ref, ranges, slots->num_ranges);
  rm_free(ranges);
  RS_LOG_ASSERT_ALWAYS(job != NULL, "Cannot register disk slot cleanup at trim notification");
  scheduleDiskJob(job);
}

void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots) {
  Indexes_ForEachSpec(SearchDisk_IsEnabled() ? scheduleDiskSlotCleanup : scheduleSlotCleanup,
                      (void *)slots);
}
