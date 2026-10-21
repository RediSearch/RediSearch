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

typedef struct {
  WeakRef spec_ref;
  // Owned by the spec's DocTable, which frees it if the spec is dropped first; only touched
  // after the weak ref was promoted.
  PendingSlotDeletion *pending;
  SlotCleanupCursor cursor;
} SlotCleanupCtx;

arrayof(t_docId) SlotCleanup_CollectDocIds(IndexSpec *spec, const PendingSlotDeletion *pending,
                                           SlotCleanupCursor *cursor) {
  const DocTable *t = &spec->docs;
  arrayof(t_docId) docIds = array_new(t_docId, 64);

  while (!SlotCleanup_CursorDone(cursor) && array_len(docIds) < SLOT_CLEANUP_CHUNK_DOC_LIMIT) {
    IndexSpec_LockRead(spec);
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
        if (dmd->id <= pending->maxDocId && SlotRangeArray_ContainsSlot(pending->slots, dmd->slot)) {
          array_append(docIds, dmd->id);
        }
      }
      ++cursor->nextBucket;
    }
    IndexSpec_Unlock(spec);
  }
  return docIds;
}

void SlotCleanup_ApplyDeletes(IndexSpec *spec, arrayof(t_docId) docIds) {
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
  // Until the task ends, queries still scan the deleted docs and filter them out, so finishing it
  // matters more than reclaiming memory.
  if (CleanPool_AddWork(SlotCleanup_Run, ctx) != 0) {
    // Run it here rather than leave the docs hidden but never deleted. Later chunks still move to
    // the pool once it accepts work again.
    RedisModule_Log(RSDummyContext, "warning", "Could not queue slot cleanup, running it on the main thread");
    SlotCleanup_Run(ctx);
  }
}

void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots) {
  // Pending deletions only filter the in-memory DocTable, and disk mode does not reshard.
  if (SearchDisk_IsEnabled()) {
    return;
  }
  Indexes_ForEachSpec(scheduleSlotCleanup, (void *)slots);
}
