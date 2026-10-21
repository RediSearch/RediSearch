/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "slot_cleanup.h"

#include <sys/param.h>

#include "indexes.h"
#include "rmalloc.h"
#include "search_disk.h"
#include "slot_ranges.h"

typedef struct {
  WeakRef spec_ref;
  // Owned by the spec's DocTable, which frees it if the spec is dropped first; only touched
  // after the weak ref was promoted.
  PendingSlotDeletion *pending;
} SlotCleanupCtx;

arrayof(t_docId) SlotCleanup_CollectDocIds(IndexSpec *spec, const PendingSlotDeletion *pending) {
  const DocTable *t = &spec->docs;
  arrayof(t_docId) docIds = array_new(t_docId, 64);
  size_t nextBucket = 0;
  size_t bucketLimit = 0;

  for (;;) {
    IndexSpec_LockRead(spec);
    // Every doc the deletion covers already existed when it was announced, so it sits in a bucket
    // below the capacity seen now. Buckets the table grows later only hold newer docs.
    if (nextBucket == 0) {
      bucketLimit = t->cap;
    }
    size_t scanned = 0;
    while (nextBucket < bucketLimit && scanned < SLOT_CLEANUP_SCAN_DOC_LIMIT) {
      for (const RSDocumentMetadata *dmd = t->buckets[nextBucket].root; dmd; dmd = dmd->nextInChain) {
        ++scanned;
        if (dmd->id <= pending->maxDocId && SlotRangeArray_ContainsSlot(pending->slots, dmd->slot)) {
          array_append(docIds, dmd->id);
        }
      }
      ++nextBucket;
    }
    IndexSpec_Unlock(spec);
    if (nextBucket >= bucketLimit) {
      return docIds;
    }
  }
}

void SlotCleanup_ApplyDeletes(IndexSpec *spec, arrayof(t_docId) docIds) {
  const size_t n = array_len(docIds);
  for (size_t start = 0; start < n; start += SLOT_CLEANUP_APPLY_DOC_LIMIT) {
    const size_t end = MIN(start + SLOT_CLEANUP_APPLY_DOC_LIMIT, n);
    IndexSpec_IncrActiveWrites(spec);
    IndexSpec_LockWrite(spec);
    for (size_t i = start; i < end; ++i) {
      IndexSpec_DeleteDocById_Unsafe(spec, docIds[i]);
    }
    IndexSpec_DecrActiveWrites(spec);
    IndexSpec_Unlock(spec);
  }
}

static void SlotCleanup_Run(void *arg) {
  SlotCleanupCtx *ctx = arg;
  StrongRef spec_ref = WeakRef_Promote(ctx->spec_ref);
  IndexSpec *spec = StrongRef_Get(spec_ref);
  if (spec) {
    arrayof(t_docId) docIds = SlotCleanup_CollectDocIds(spec, ctx->pending);
    SlotCleanup_ApplyDeletes(spec, docIds);
    array_free(docIds);

    IndexSpec_LockWrite(spec);
    DocTable_RemovePendingDeletion(&spec->docs, ctx->pending);
    IndexSpec_Unlock(spec);
    StrongRef_Release(spec_ref);
  }
  WeakRef_Release(ctx->spec_ref);
  rm_free(ctx);
}

static void scheduleSlotCleanup(IndexSpec *spec, void *slots) {
  IndexSpec_LockWrite(spec);
  // An empty index has nothing to hide, and a node would only slow lookups until its task runs.
  if (spec->stats.scoring.numDocuments == 0) {
    IndexSpec_Unlock(spec);
    return;
  }
  PendingSlotDeletion *pending = DocTable_AddPendingDeletion(&spec->docs, slots);
  IndexSpec_Unlock(spec);

  SlotCleanupCtx *ctx = rm_malloc(sizeof(*ctx));
  ctx->spec_ref = StrongRef_Demote(spec->own_ref);
  ctx->pending = pending;
  // Until the task ends, queries still scan the deleted docs and filter them out, so finishing it
  // matters more than reclaiming memory.
  CleanPool_AddWork(SlotCleanup_Run, ctx);
}

void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots) {
  // Pending deletions only filter the in-memory DocTable, and disk mode does not reshard.
  if (SearchDisk_IsEnabled()) {
    return;
  }
  Indexes_ForEachSpec(scheduleSlotCleanup, (void *)slots);
}
