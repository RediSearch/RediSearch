# Slot-Based Document Deletion

## Overview

When Redis cluster slots are migrated away, we need to quickly mark all documents belonging to those slots as "deleted" without performing a costly linear scan. This document describes the design for fast slot-based deletion in the DocTable.

## Problem Statement

Redis will notify us (via `RedisModuleSlotRangeArray`) when slots need to be deleted. We need to:

1. **Instantly** make documents in those slots unreachable (O(1) per slot, not O(n) per document)
2. Keep `DocTable_Borrow` fast — it's on the critical query path
3. Actual memory cleanup can happen asynchronously via background task + fork GC

### Use Cases

1. **ASM (Async Slot Migration) Trimming** — Primary use case, integrated with existing ASM mechanism
2. **Future `SFLUSH` notification** — Redis `SFLUSH` command for manual slot cleanup; we receive notification

### Rejected Approach

Linear scan + per-key slot hash computation is O(n) and unacceptable:
```c
// BAD: O(n) scan with hash per document
DOCTABLE_FOREACH(t, {
  int slot = RedisModule_ClusterKeySlotC(dmd->keyPtr, sdslen(dmd->keyPtr));
  if (SlotRangeArray_ContainsSlot(deletedSlots, slot)) {
    DocTable_Pop(t, dmd->keyPtr, sdslen(dmd->keyPtr));
  }
})
```

## Design Overview

### Core Idea

1. **Pre-store slot number** in each `RSDocumentMetadata` at insertion time (computed once)
2. **Maintain a list of pending deletions** in DocTable — each entry has `(slots, maxDocId)`
3. **Capture `maxDocId`** at notification time — documents with id > maxDocId are newer and must be kept
4. **Background cleanup** removes documents incrementally, skipping any doc with id > maxDocId
5. **Query-time filtering**: A document is filtered if ANY pending deletion matches its slot AND `doc.id <= pending.maxDocId`

### Atomic Deletion via maxDocId Cutoff

Document IDs are yielded from a monotonically increasing counter. Any document with `id > maxDocId` was written **after** the notification and must be preserved.

This is simple and efficient:
- At notification: `maxDocId = spec->docs.maxId`
- During cleanup: skip any document where `dmd->id > maxDocId`
- Query-time: filter documents where `doc.id <= ANY(pending.maxDocId)` for matching slots

### Why Pending List Instead of Bitmap?

We need a **list** of pending deletions (not a single bitmap) because each deletion request has its own `maxDocId` cutoff:
- If deletion A (slots 1-10, maxDocId=100) is processing, then deletion B (slots 5-15, maxDocId=200) arrives
- Each must track its own cutoff independently
- A doc in slot 7 with id=150 should be filtered by B but not by A

Within each entry, we store **slot ranges** rather than converting to a bitmap because:
- We already receive `RedisModuleSlotRangeArray` from Redis — no conversion needed
- Slot deletions are typically of contiguous ranges
- A ranges array is ~4 bytes per range vs 2KB fixed for a bitmap
- The pending list is temporary (cleared after cleanup completes)

### Key Insight: Zero Memory Overhead for Slot Storage

The `RSDocumentMetadata` struct has **2 bytes of padding** after `ref_count` due to alignment:

```
offset 28: uint16_t ref_count (2 bytes)
offset 30: [2 BYTES PADDING] ◄── slot fits here for FREE
offset 32: struct RSSortingVector *sortVector (8 bytes, 8-byte aligned)
```

Storing `uint16_t slot` uses this padding — **zero additional memory per document**.

---

## Data Structure Changes

### RSDocumentMetadata (`src/redisearch.h`)

```c
typedef struct RSDocumentMetadata_s {
  t_docId id;
  char *keyPtr;
  float score;
  uint32_t maxTermFreq : 24;
  RSDocumentFlags flags : 8;
  uint32_t docLen : 24;
  DocumentType type : 8;
  uint16_t ref_count;

  uint16_t slot;  // NEW: Redis cluster slot [0-16383], uses existing padding

  struct RSSortingVector *sortVector;
  struct RSByteOffsets *byteOffsets;
  struct RSDocumentMetadata_s *nextInChain;
  RSPayload *payload;
} RSDocumentMetadata;
```

**Memory overhead: 0 bytes** (uses existing struct padding)

### DocTable (`src/doc_table.h`)

```c
// A single pending slot deletion request (linked list node)
typedef struct PendingSlotDeletion {
  RedisModuleSlotRangeArray *slots;  // Which slots to delete
  t_docId maxDocId;                  // Only delete docs with id <= this
  struct PendingSlotDeletion *next;  // Next in linked list
} PendingSlotDeletion;

typedef struct {
  // ... existing fields ...

  // Pending slot deletions - linked list of independent deletion requests
  // A document is filtered if ANY entry matches (slot in ranges AND id <= maxDocId)
  PendingSlotDeletion *pendingSlotDeletions;  // Head of linked list
} DocTable;
```

**Memory overhead:** 8 bytes (list head pointer) normally, ~48 bytes per active deletion request.

**Why a linked list?**
- Each deletion request may have a different `maxDocId` cutoff
- Cleanup jobs hold direct pointers to their nodes — linked list nodes have stable addresses
- Removal of one node doesn't invalidate pointers to other nodes
- Typically 0-1 entries, so traversal is fast

---

## API

### DocTable Level

```c
// Add a pending slot deletion — captures maxDocId internally, returns handle for cleanup job
// Caller must hold write lock on spec
PendingSlotDeletion *DocTable_AddPendingDeletion(DocTable *t,
                                                   const RedisModuleSlotRangeArray *slots);

// Remove a completed pending deletion
// Caller must hold write lock on spec
void DocTable_RemovePendingDeletion(DocTable *t, PendingSlotDeletion *pending);

// Check if any deletions are pending (fast path check)
static inline bool DocTable_HasPendingDeletions(const DocTable *t) {
  return t->pendingSlotDeletions != NULL;
}

// Check if document should be filtered due to pending deletions
// Returns true if doc is in a pending-deleted slot AND id <= that deletion's maxDocId
static inline bool DocTable_IsDocPendingDeletion(const DocTable *t,
                                                   uint16_t slot, t_docId docId) {
  for (PendingSlotDeletion *p = t->pendingSlotDeletions; p != NULL; p = p->next) {
    if (docId <= p->maxDocId && SlotInRanges(slot, p->slots)) {
      return true;
    }
  }
  return false;
}
```

### Index-Wide (All Specs)

```c
// Context for slot cleanup tasks (passed to background thread)
typedef struct {
  WeakRef spec_ref;                  // Weak ref - promoted at execution start
  PendingSlotDeletion *pending;      // Direct pointer to pending deletion node
} SlotCleanupCtx;

// Background cleanup task
void SlotCleanupTask_Run(SlotCleanupCtx *ctx);

// Delete documents in specified slots across all indexes
// This is the unified entry point — caller handles external concerns (slots_tracker, etc.)
void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots);
```

---

## Implementation Details

### DocTable_Put — Store slot at insertion

```c
RSDocumentMetadata *DocTable_Put(DocTable *t, const char *s, size_t n, ...) {
  // ... existing allocation ...

  // Compute and store slot at insertion (uses existing padding, zero cost)
  dmd->slot = (uint16_t)RedisModule_ClusterKeySlotC(s, n);

  // ... rest of existing code ...
}
```

### DocTable_GetOwn — Check pending deletions

```c
static RSDocumentMetadata *DocTable_GetOwn(const DocTable *t, t_docId docId) {
  // ... existing validation and bucket lookup ...

  DMDChain *dmdChain = &t->buckets[bucketIndex];
  for (RSDocumentMetadata *dmd = dmdChain->root; dmd != NULL; dmd = dmd->nextInChain) {
    if (dmd->id == docId) {
      // Existing deletion check
      if (dmd->flags & Document_Deleted) return NULL;

      // NEW: Check if doc is pending deletion (slot + maxDocId check)
      if (DocTable_IsDocPendingDeletion(t, dmd->slot, dmd->id)) return NULL;

      return dmd;
    }
  }
  return NULL;
}
```

### DocTable_AddPendingDeletion

```c
PendingSlotDeletion *DocTable_AddPendingDeletion(DocTable *t,
                                                   const RedisModuleSlotRangeArray *slots) {
  // Caller must hold write lock on spec

  PendingSlotDeletion *pending = rm_malloc(sizeof(*pending));
  pending->slots = SlotRangeArray_Clone(slots);
  pending->maxDocId = t->maxId;  // Capture current maxDocId

  // Prepend to linked list (O(1))
  pending->next = t->pendingSlotDeletions;
  t->pendingSlotDeletions = pending;

  return pending;
}
```

### DocTable_RemovePendingDeletion

```c
void DocTable_RemovePendingDeletion(DocTable *t, PendingSlotDeletion *pending) {
  // Caller must hold write lock on spec

  // Find and unlink from list
  PendingSlotDeletion **pp = &t->pendingSlotDeletions;
  while (*pp != NULL) {
    if (*pp == pending) {
      *pp = pending->next;  // Unlink
      break;
    }
    pp = &(*pp)->next;
  }

  // Free resources
  rm_free(pending->slots);
  rm_free(pending);
}
```

---

## Async Cleanup Architecture

### Thread Safety Consideration

The global specs dictionary (`specDict_g`) is **not thread-safe** and should only be accessed from the main thread or with the GIL held. Therefore:

- **Main thread** iterates specs and creates one cleanup job per spec
- **Background thread** (`cleanPool`) processes individual spec cleanup jobs
- Each job holds a `WeakRef` to its spec, promoted to `StrongRef` at execution start
- If promotion fails (spec was dropped), the job is skipped

### Thread Pool: `cleanPool`

Use existing `cleanPool` (defined in `src/spec.c`):
- Single-threaded pool for cleanup tasks
- Already used for `IndexSpec_FreeUnlinkedData`
- Low priority, won't interfere with queries

---

## Unified Slot Deletion Implementation

All slot deletion requests use the same mechanism. Callers (event handlers) may do additional
work like updating `slots_tracker`, but the deletion mechanism itself is identical.

### Main Thread Handler

```c
void Indexes_DeleteSlots(const RedisModuleSlotRangeArray *slots) {
  // Must be called from main thread

  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;

  while ((entry = dictNext(iter)) != NULL) {
    IndexSpec *spec = (IndexSpec *)dictGetVal(entry);

    // Add pending deletion (captures maxDocId internally)
    IndexSpec_WriteLock(spec);
    PendingSlotDeletion *pending = DocTable_AddPendingDeletion(&spec->docs, slots);
    IndexSpec_WriteUnlock(spec);

    // Schedule background cleanup with weak ref + direct pointer to pending node
    SlotCleanupCtx *ctx = rm_malloc(sizeof(*ctx));
    ctx->spec_ref = IndexSpec_GetWeakRef(spec);
    ctx->pending = pending;

    redisearch_thpool_add_work(cleanPool,
                               (redisearch_thpool_proc)SlotCleanupTask_Run,
                               ctx, THPOOL_PRIORITY_LOW);
  }
  dictReleaseIterator(iter);
}
```

### Background Cleanup Task

```c
#define CLEANUP_BATCH_SIZE 64

// Helper: Check if slot is in the given slot ranges
static bool SlotInRanges(uint16_t slot, const RedisModuleSlotRangeArray *slots) {
  for (int32_t i = 0; i < slots->num_ranges; i++) {
    if (slot >= slots->ranges[i].start && slot <= slots->ranges[i].end) return true;
  }
  return false;
}

// Cleanup a batch of buckets
static void DocTable_CleanupSlots_Batch(IndexSpec *spec, PendingSlotDeletion *pending,
                                         size_t startBucket, size_t endBucket) {
  DocTable *t = &spec->docs;

  // Phase 1: COLLECT keys with READ lock
  IndexSpec_ReadLock(spec);

  arrayof(sds) keysToDelete = array_new(sds, 64);
  for (size_t i = startBucket; i < endBucket; i++) {
    DMDChain *chain = &t->buckets[i];
    for (RSDocumentMetadata *dmd = chain->root; dmd; dmd = dmd->nextInChain) {
      // Skip docs written after notification
      if (dmd->id > pending->maxDocId) continue;

      // Check if slot is in deletion range
      if (SlotInRanges(dmd->slot, pending->slots)) {
        array_append(keysToDelete, sdsdup(dmd->keyPtr));
      }
    }
  }
  IndexSpec_ReadUnlock(spec);

  // Phase 2: DELETE with brief WRITE lock
  if (array_len(keysToDelete) > 0) {
    IndexSpec_WriteLock(spec);
    for (size_t i = 0; i < array_len(keysToDelete); i++) {
      IndexSpec_DeleteDocByKey(spec, keysToDelete[i], sdslen(keysToDelete[i]));
    }
    IndexSpec_WriteUnlock(spec);
  }

  for (size_t i = 0; i < array_len(keysToDelete); i++) sdsfree(keysToDelete[i]);
  array_free(keysToDelete);
}

// Free cleanup context resources
static void SlotCleanupCtx_Free(SlotCleanupCtx *ctx, StrongRef strong_ref) {
  if (StrongRef_Get(strong_ref)) {
    StrongRef_Release(strong_ref);
  }
  WeakRef_Release(ctx->spec_ref);
  rm_free(ctx);
}

void SlotCleanupTask_Run(SlotCleanupCtx *ctx) {
  // Promote weak ref to strong ref — if index was dropped, skip cleanup
  StrongRef strong_ref = WeakRef_Promote(ctx->spec_ref);
  IndexSpec *spec = StrongRef_Get(strong_ref);
  if (!spec) goto cleanup;  // Index was dropped, nothing to do

  DocTable *t = &spec->docs;
  PendingSlotDeletion *pending = ctx->pending;

  // Process all buckets in batches
  for (size_t startBucket = 0; startBucket < t->cap; startBucket += CLEANUP_BATCH_SIZE) {
    size_t endBucket = MIN(startBucket + CLEANUP_BATCH_SIZE, t->cap);
    DocTable_CleanupSlots_Batch(spec, pending, startBucket, endBucket);
  }

  // Remove our entry from pending deletions list
  IndexSpec_WriteLock(spec);
  DocTable_RemovePendingDeletion(t, pending);
  IndexSpec_WriteUnlock(spec);

cleanup:
  SlotCleanupCtx_Free(ctx, strong_ref);
}
```

---

## Flow Diagram

### Unified Slot Deletion Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│ 1. Redis Notification (any slot deletion event)                            │
│    TRIM_BACKGROUND: Slots leaving this node                                │
│    SFLUSH: Slot data flushed, slots still ours                             │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 2. Event Handler [MAIN THREAD]                                             │
│                                                                            │
│    // Core deletion mechanism (identical for all events)                   │
│    Indexes_DeleteSlots(slots);                                             │
│                                                                            │
│    // Event-specific external updates (caller responsibility)              │
│    if (TRIM_BACKGROUND) {                                                  │
│      slots_tracker_mark_partially_available_slots(slots);                  │
│      slots_tracker_remove_deleted_slots(slots);                            │
│    }                                                                       │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 3. Indexes_DeleteSlots(slots)                                              │
│    [MAIN THREAD - Synchronous]                                             │
│                                                                            │
│    FOR each IndexSpec in specDict_g:                                       │
│      WriteLock(spec)                                                       │
│      maxDocId = spec->docs.maxId                                           │
│      DocTable_AddPendingDeletion(&spec->docs, slots, maxDocId)             │
│      WriteUnlock(spec)                                                     │
│      Schedule SlotCleanupTask(spec, slots, maxDocId)                       │
│                                                                            │
│    ✓ INSTANT: DocTable queries now filter docs matching pending deletions  │
│               (slot in ranges AND doc.id <= maxDocId)                      │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (N jobs, one per spec)
┌────────────────────────────────────────────────────────────────────────────┐
│ 4. SlotCleanupTask_Run() [cleanPool - Background Thread]                   │
│                                                                            │
│    FOR each bucket batch (64 buckets):                                     │
│      Phase 1 - COLLECT keys with ReadLock                                  │
│                Check slot in ranges && id <= maxDocId                      │
│      Phase 2 - DELETE with WriteLock                                       │
│                Delete doc, update stats, trigger GC, VecSim/Geometry       │
│                                                                            │
│    Remove this entry from pendingSlotDeletions list                        │
│                                                                            │
│    ✓ Old documents deleted, new documents (id > maxDocId) preserved        │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 5. Fork GC (Later, Normal Operation)                                       │
│    - Cleans inverted index entries for deleted documents                   │
│    - Frees remaining memory                                                │
└────────────────────────────────────────────────────────────────────────────┘
```

### Multiple Concurrent Deletions

Each deletion request creates its own entry with its own `maxDocId` cutoff:

```
Timeline example:
  T0: Delete slots 1-10, maxDocId = 100
      pendingSlotDeletions = [(slots:1-10, maxDocId:100)]

  T5: Delete slots 5-15, maxDocId = 200 (cleanup for T0 still running)
      pendingSlotDeletions = [(slots:1-10, maxDocId:100), (slots:5-15, maxDocId:200)]

  T10: Query for doc D in slot 7:
       - Check (slots:1-10, maxDocId:100): slot 7 in range, check doc.id
         * If doc.id <= 100 → filtered
         * If doc.id > 100 → check next pending
       - Check (slots:5-15, maxDocId:200): slot 7 in range, check doc.id
         * If doc.id <= 200 → filtered
         * If doc.id > 200 → visible
```

### New Document Protection

```
Timeline example:
  T0: Delete slots 1-10, maxDocId = 100
  T1: New write: doc D(key="foo", slot=5, id=150)
  T2: Query for doc D:
      - Check pending (slots:1-10, maxDocId:100)
      - slot 5 in range ✓
      - id 150 > maxDocId 100 → NOT filtered
      - Doc D visible ✓
```

---

## ASM (Atomic Slot Migration) Integration

This slot-based deletion mechanism integrates with the existing ASM state machine.

### Current Mode: Event-Driven Trimming

The current ASM trimming flow uses Redis keyspace notifications:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_STARTED                │
│   └─> ASM_StateMachine_StartTrim(slots)                                 │
│       - Mark slots as partially available in slots_tracker              │
│       - Update key_space_version                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Per-Key Notifications (trimmed_cmd, key_trimmed_cmd)                    │
│   └─> Indexes_DeleteMatchingWithSchemaRules(key)                        │
│       - Delete document from matching indexes one by one                │
│       - O(n) total work, but spread over time                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_COMPLETED              │
│   └─> ASM_StateMachine_CompleteTrim(slots)                              │
│       - Remove slots from partially_available set                       │
└─────────────────────────────────────────────────────────────────────────┘
```

**Characteristics:**
- Documents deleted one-by-one via keyspace notifications
- Deletion spread over time during Redis trim operation
- Module receives per-key callbacks

### New Mode: Background Trimming (TRIM_BACKGROUND)

The new mode receives a **single notification** with the full slot range:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_BACKGROUND             │
│   └─> Receives RedisModuleSlotRangeArray *slots                         │
│       - No per-key notifications                                        │
│       - No separate TRIM_COMPLETED event                                │
│       - Module responsible for cleanup                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Event Handler:                                                          │
│   Indexes_DeleteSlots(slots);                 ◀── Core deletion         │
│   slots_tracker_mark_partially_available_slots(slots);                  │
│   slots_tracker_remove_deleted_slots(slots);  ◀── Topology update       │
└─────────────────────────────────────────────────────────────────────────┘
```

**Characteristics:**
- Single notification with slot ranges
- Module handles deletion internally via `Indexes_DeleteSlots`
- **Instant filtering** via pending deletions list
- `slots_tracker` updated synchronously — no async completion callback

### Event Handler Integration

```c
void ClusterSlotMigrationTrimEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                    uint64_t subevent, void *data) {
  RedisModuleClusterSlotMigrationTrimInfo *info = data;
  RedisModuleSlotRangeArray *slots = info->slots;

  switch (subevent) {

    // Current mode: Event-driven trimming
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_STARTED:
      workersThreadPool_OnEventStart();
      ASM_StateMachine_StartTrim(slots);
      break;

    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_COMPLETED:
      workersThreadPool_OnEventEnd(false);
      ASM_StateMachine_CompleteTrim(slots);
      break;

    // New mode: Background trimming (single notification)
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_BACKGROUND:
      // Core deletion mechanism
      Indexes_DeleteSlots(slots);
      // Topology update (slots leaving this node)
      slots_tracker_mark_partially_available_slots(slots);
      slots_tracker_remove_deleted_slots(slots);
      break;
  }
}

void SFlushEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModuleSFlushInfo *info = (RedisModuleSFlushInfo *)data;
  RedisModuleSlotRangeArray *slots = info->slots;

  // Core deletion mechanism (same as TRIM_BACKGROUND)
  Indexes_DeleteSlots(slots);
  // No topology update — slots still ours
}
```

**Key insight**: Both TRIM_BACKGROUND and SFLUSH use the same `Indexes_DeleteSlots` function.
The only difference is what the event handler does externally (slots_tracker updates).

---

## Complexity Analysis

| Operation | Complexity | Lock | Duration |
|-----------|------------|------|----------|
| `DocTable_AddPendingDeletion` | O(1) | Write | Brief |
| `Indexes_DeleteSlots` | O(num_specs) | Write per spec | Brief per spec |
| `DocTable_Borrow` (normal) | O(chain_length) | Read | — |
| `DocTable_Borrow` (during cleanup) | O(chain_length × num_pending) | Read | — |
| Cleanup collection phase | O(batch_size × avg_chain) | **Read** | Per batch |
| Cleanup deletion phase | O(keys_to_delete) | Write | Brief, only if keys found |
| Total background cleanup | O(total_documents) | Mostly read | Yielding |

---

## Memory Summary

| Component | Normal Operation | During Cleanup |
|-----------|------------------|----------------|
| Per DMD (`slot` field) | 0 bytes (uses padding) | 0 bytes |
| Per DocTable (pending list pointer) | 8 bytes | 8 bytes |
| Per pending deletion entry | 0 bytes | ~40 bytes (slots array + maxDocId) |

Note: Typically 0-2 pending deletions at any time. Memory freed when cleanup completes.

---

## Locking Strategy

| Operation | Lock Type | Notes |
|-----------|-----------|-------|
| `DocTable_AddPendingDeletion` | Write | Held briefly, per spec |
| `DocTable_IsDocPendingDeletion` | Read | Part of `DocTable_Borrow` |
| Cleanup collection (iteration) | **Read** | Queries run concurrently |
| Cleanup deletion (mutations) | Write | Brief, only when keys to delete |
| `DocTable_RemovePendingDeletion` | Write | Held briefly |

The two-phase cleanup approach minimizes query blocking:
- **Collection phase** uses read lock — queries proceed normally
- **Deletion phase** uses brief write lock — only for actual mutations
- If no documents to delete in a batch, write lock is never taken

---

## Edge Cases

### Multiple Deletion Events During Cleanup

If a new slot deletion notification arrives while cleanup is in progress:

1. Main thread adds new entry to `pendingSlotDeletions` list (instant)
2. New cleanup job queues in `cleanPool`
3. Current job continues with its own `maxDocId` cutoff
4. New job processes with its own `maxDocId` cutoff when it runs

Each entry in the pending list is independent — no merging, no conflicts.

### Spec Created/Dropped During Cleanup

- **New spec**: Won't have pending deletions — that's OK, it doesn't have documents from those slots
- **Dropped spec**: `WeakRef_Promote` fails, cleanup job is skipped safely

### Overlapping Slot Ranges

If deletion A (slots 1-10, maxDocId=100) and deletion B (slots 5-15, maxDocId=200) overlap:

- Query for doc in slot 7, id=150:
  - Check A: slot 7 in range, but id 150 > maxDocId 100 → not filtered by A
  - Check B: slot 7 in range, id 150 ≤ maxDocId 200 → **filtered by B**
- Query for doc in slot 7, id=250:
  - Check A: id 250 > maxDocId 100 → not filtered
  - Check B: id 250 > maxDocId 200 → not filtered
  - **Doc visible**

This correctly handles the case where a newer deletion request should filter
documents that an older request wouldn't.

### Race Between Collection and Deletion Phases

Since collection (read lock) and deletion (write lock) are separate:

- **Document deleted between phases**: Re-check slot + docId still valid → skip if gone
- **New document with same key inserted**: New doc has id > maxDocId → skip deletion ✓
- **Document unchanged (id ≤ maxDocId)**: Delete correctly ✓

---

## Testing Considerations

1. **Unit tests**: `DocTable_AddPendingDeletion`, `DocTable_IsDocPendingDeletion`, `DocTable_RemovePendingDeletion`
2. **Integration test**: Full flow with multiple specs
3. **Concurrency test**: Queries during cleanup
4. **Memory test**: Verify pending list allocation/deallocation
5. **Edge case**: Multiple overlapping deletion events with different maxDocId
6. **ASM test**: Both TRIM_STARTED/COMPLETED and TRIM_BACKGROUND modes
7. **New document protection tests**:
   - New document written during cleanup remains visible (id > maxDocId)
   - New document with same key as old document survives cleanup (id > maxDocId)
   - Old documents in target slots are deleted correctly (id ≤ maxDocId)
8. **Concurrent deletions tests**:
   - Two deletions with different maxDocId for overlapping slots
   - Each cleanup job only deletes docs within its own maxDocId cutoff
   - Pending list correctly updated after each cleanup completes

---

## Future Considerations

- **Batch size tuning**: `CLEANUP_BATCH_SIZE` may need adjustment based on workload
- **Progress tracking**: Could add metrics for monitoring cleanup progress
- **Cancellation**: If needed, could add early termination for cleanup jobs

---

## Summary

| Aspect | Design Choice |
|--------|---------------|
| Slot storage | Uses existing DMD padding (0 bytes) |
| Deletion tracking | Linked list of pending deletions (slots + maxDocId per node) |
| Marking speed | O(1) — prepend to linked list |
| Query overhead | O(pending_count) — typically 0-2 entries |
| New doc protection | `maxDocId` captured in `DocTable_AddPendingDeletion()` |
| Thread pool | `cleanPool` (existing single-thread pool) |
| Job model | **One job per spec per deletion request** |
| Cleanup context | Direct pointer to pending node (no data duplication) |
| Entry point | `Indexes_DeleteSlots()` — unified for all events |
| Event-specific | Caller handles slots_tracker, etc. |
| Concurrent deletions | Each has its own maxDocId, no merging |

This design achieves:
- ✅ **Instant** slot filtering (not O(n) — just prepend to linked list)
- ✅ **Zero** steady-state memory overhead per document
- ✅ **Minimal** query overhead during cleanup (O(pending_count) check, typically 0-2)
- ✅ **Simple** `maxDocId` cutoff — captured automatically, only delete/filter docs with id ≤ maxDocId
- ✅ **Unified** — single `Indexes_DeleteSlots()` for all slot deletion events
- ✅ **Correct** concurrent deletions — each has its own maxDocId, no conflicts
- ✅ **Low contention** cleanup — read lock for iteration, brief write lock only for mutations
- ✅ **Thread-safe** — main thread iterates specs, creates per-spec jobs
- ✅ **Simple** — no async completion tracking, event handler does external updates
- ✅ **Stable pointers** — linked list nodes have fixed addresses, cleanup jobs hold direct pointers
- ✅ **Full deletion** — stats, GC, VecSim, Geometry all updated correctly
- ✅ **New doc protection** — docs with id > maxDocId always visible
