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
    for (const RSDocumentMetadata *dmd = t->buckets[cursor->nextBucket].root; dmd; dmd = dmd->nextInChain) {
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
  SearchDiskSlotRange *ranges;
  size_t numRanges;
  t_docId maxDocId;
  bool queued;
  struct IndexSpecSlotTrimJob *next;
};

// Shutdown drains complete worker turns before closing disk handles.
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

static IndexSpecSlotTrimJob *newDiskJob(StrongRef ref, const SearchDiskSlotRange *ranges,
                                        size_t numRanges, t_docId maxDocId) {
  IndexSpecSlotTrimJob *job = rm_calloc(1, sizeof(*job));
  job->specRef = StrongRef_Demote(ref);
  job->ranges = rm_malloc(sizeof(*ranges) * numRanges);
  memcpy(job->ranges, ranges, sizeof(*ranges) * numRanges);
  job->numRanges = numRanges;
  job->maxDocId = maxDocId;
  return job;
}

IndexSpecSlotTrimJob *IndexSpec_RegisterDiskSlotTrim(StrongRef specRef,
                                                     const SearchDiskSlotRange *ranges,
                                                     size_t numRanges) {
  IndexSpec *spec = StrongRef_Get(specRef);
  if (!spec || !spec->diskSpec || atomic_load(&diskJobsStopped)) return NULL;
  t_docId maxDocId = 0;
  IndexSpec_LockWrite(spec);
  bool registered = SearchDisk_RegisterSlotTrim(spec->diskSpec, ranges, numRanges, &maxDocId);
  RS_LOG_ASSERT_ALWAYS(registered, "valid slot cleanup registration must succeed");
  IndexSpec_Unlock(spec);
  return newDiskJob(specRef, ranges, numRanges, maxDocId);
}

static void unlinkDiskJob(IndexSpecSlotTrimJob *job) {
  IndexSpecSlotTrimJob **link = &diskJobs;
  while (*link && *link != job) link = &(*link)->next;
  if (*link) *link = job->next;
}

typedef struct {
  IndexSpec *spec;
  IndexSpecSlotTrimJob *job;
} DiskSlotTrimContext;

static bool trimMemoryIndexes(void *ctx, const t_docId *ids, size_t numIds, uint64_t totalDocLen,
                              SearchDiskWriteBatchHandle *batch, bool exhausted) {
  DiskSlotTrimContext *trim = ctx;
  IndexSpec *spec = trim->spec;
  pthread_mutex_lock(&diskApplyLock);
  if (diskJobsPaused) {
    SearchDisk_AbortWriteBatch(batch);
    pthread_mutex_unlock(&diskApplyLock);
    return false;
  }
  IndexSpec_IncrActiveWrites(spec);
  IndexSpec_LockWrite(spec);
  bool applied = SearchDisk_CommitWriteBatch(batch);
  if (applied) {
    RS_LOG_ASSERT_ALWAYS(spec->stats.scoring.numDocuments >= numIds,
                         "slot cleanup exceeds the document count");
    RS_LOG_ASSERT_ALWAYS(spec->stats.scoring.totalDocsLen >= totalDocLen,
                         "slot cleanup exceeds the total document length");
    for (size_t i = 0; i < numIds; ++i) IndexSpec_RemoveDocFromMemoryIndexes(spec, ids[i]);
    spec->stats.scoring.numDocuments -= numIds;
    spec->stats.scoring.totalDocsLen -= totalDocLen;
    IndexSpec_DeferGCDeletes(spec, numIds);
    if (exhausted) {
      // A later registration of these ranges must not match this completed worker.
      pthread_mutex_lock(&diskJobListLock);
      unlinkDiskJob(trim->job);
      pthread_mutex_unlock(&diskJobListLock);
    }
  }
  IndexSpec_Unlock(spec);
  IndexSpec_DecrActiveWrites(spec);
  pthread_mutex_unlock(&diskApplyLock);
  return applied;
}

IndexSpecDiskSlotTrimResult IndexSpec_RunDiskSlotTrim(IndexSpecSlotTrimJob *job) {
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
    SearchDiskWriteBatchHandle *batch = SearchDisk_CreateWriteBatch(spec->diskSpec);
    if (batch) {
      SearchDiskSlotTrimResult status =
          SearchDisk_RunSlotTrimBatch(spec->diskSpec, batch, job->ranges, job->numRanges,
                                      job->maxDocId, SLOT_CLEANUP_CHUNK_DOC_LIMIT,
                                      trimMemoryIndexes, &context);
      result = status == SEARCH_DISK_TRIM_FINISHED ? INDEX_DISK_TRIM_FINISHED
               : status == SEARCH_DISK_TRIM_MORE   ? INDEX_DISK_TRIM_MORE
                                                   : INDEX_DISK_TRIM_RETRY;
      SearchDisk_FreeWriteBatch(batch);
    } else {
      result = INDEX_DISK_TRIM_RETRY;
    }
  }
  if (running.rm) StrongRef_Release(running);
  pthread_rwlock_unlock(&diskJobRunLock);
  return result;
}

void IndexSpec_FreeDiskSlotTrim(IndexSpecSlotTrimJob *job) {
  if (!job) return;
  WeakRef_Release(job->specRef);
  rm_free(job->ranges);
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

static void DiskSlotCleanup_Run(void *arg) {
  IndexSpecSlotTrimJob *job = arg;
  IndexSpecDiskSlotTrimResult result = IndexSpec_RunDiskSlotTrim(job);
  pthread_mutex_lock(&diskJobListLock);
  job->queued = false;
  if (result == INDEX_DISK_TRIM_FINISHED || result == INDEX_DISK_TRIM_CANCELLED ||
      atomic_load(&diskJobsStopped)) {
    if (result != INDEX_DISK_TRIM_FINISHED) unlinkDiskJob(job);
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
  for (IndexSpecSlotTrimJob *existing = diskJobs; existing; existing = existing->next) {
    if (existing->specRef.rm != job->specRef.rm || existing->maxDocId != job->maxDocId ||
        existing->numRanges != job->numRanges) continue;
    size_t i = 0;
    while (i < job->numRanges && existing->ranges[i].start == job->ranges[i].start &&
           existing->ranges[i].end == job->ranges[i].end) ++i;
    if (i == job->numRanges) {
      pthread_mutex_unlock(&diskJobListLock);
      IndexSpec_FreeDiskSlotTrim(job);
      return;
    }
  }
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

static void resumeDiskTrim(void *ctx, const SearchDiskSlotRange *ranges, size_t numRanges,
                           t_docId maxDocId) {
  IndexSpec *spec = ctx;
  scheduleDiskJob(newDiskJob(spec->own_ref, ranges, numRanges, maxDocId));
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
  if (job) scheduleDiskJob(job);
}

void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots) {
  Indexes_ForEachSpec(SearchDisk_IsEnabled() ? scheduleDiskSlotCleanup : scheduleSlotCleanup,
                      (void *)slots);
}
