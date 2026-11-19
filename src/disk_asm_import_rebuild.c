/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "disk_asm_import_rebuild.h"

#include <errno.h>
#include <string.h>

#include "asm_state_machine.h"
#include "doc_id_meta.h"
#include "doc_types.h"
#include "indexes.h"
#include "indexes_asyncscan.h"
#include "indexes_scan.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "search_disk.h"
#include "slot_cleanup.h"
#include "spec.h"
#include "util/arr/arr.h"
#include "util/references.h"

typedef enum {
  // The gate is held; waiting for the overlay to end.
  DISK_ASM_IMPORT_AWAITING_OVERLAY_END,
  // The sweep is queued or running.
  DISK_ASM_IMPORT_SCANNING,
  // The slots are rebuilt and the gate released.
  DISK_ASM_IMPORT_REBUILT,
  // The rebuild cannot complete: the attempt is aborted, and the gate, if held, is kept so it
  // cannot complete meanwhile.
  DISK_ASM_IMPORT_REBUILD_FAILED,
  // The attempt ended (IMPORT_COMPLETED / IMPORT_FAILED).
  DISK_ASM_IMPORT_COMPLETED,
  DISK_ASM_IMPORT_FAILED,
} DiskASMImportPhase;

static const char *phaseName(DiskASMImportPhase phase) {
  switch (phase) {
    case DISK_ASM_IMPORT_AWAITING_OVERLAY_END:
      return "awaiting_overlay_end";
    case DISK_ASM_IMPORT_SCANNING:
      return "scanning";
    case DISK_ASM_IMPORT_REBUILT:
      return "rebuilt";
    case DISK_ASM_IMPORT_REBUILD_FAILED:
      return "rebuild_failed";
    case DISK_ASM_IMPORT_COMPLETED:
      return "completed";
    case DISK_ASM_IMPORT_FAILED:
      return "failed";
  }
  return "unknown";
}

typedef struct {
  char taskId[128];
  DiskASMImportPhase phase;
  bool gateHeld;
  bool abortRequested;
  size_t keysScanned;
  size_t keysIndexed;
  // A static string, or NULL.
  const char *lastError;
} DiskASMImportStats;

typedef struct {
  AsyncScanJob job;
  char *taskId;
  char *nameForLogs;
  RedisModuleSlotRangeArray *slots;
  DiskASMImportStats stats;
  // The participating indexes, frozen when the overlay ends. All have a specId of at most
  // frozenMaxSpecId.
  arrayof(WeakRef) specs;
  uint64_t frozenMaxSpecId;
  // The sweep task holds the coordinator from the moment it is queued until its finish.
  bool scanRunning;
  // The attempt ended while the sweep ran: the sweep stops and frees the coordinator. Read by the
  // sweep without the GIL.
  bool ended;
  // Some key's DocIdMeta could not be cleared, so the slots cannot leave the window.
  bool clearFailed;
} DiskASMImport;

// Live coordinators, in creation order.
static arrayof(DiskASMImport *) imports = NULL;
// The latest attempt that ended, for INFO once no coordinator is left.
static DiskASMImportStats lastEnded = {.taskId = "", .phase = DISK_ASM_IMPORT_COMPLETED};
static bool anyEnded = false;

static RedisModuleSlotRangeArray *copySlots(const RedisModuleSlotRangeArray *slots) {
  size_t size = sizeof(*slots) + slots->num_ranges * sizeof(slots->ranges[0]);
  RedisModuleSlotRangeArray *copy = rm_malloc(size);
  memcpy(copy, slots, size);
  return copy;
}

static void importFree(DiskASMImport *imp) {
  RS_ASSERT(!imp->scanRunning);
  if (imp->specs) {
    for (size_t i = 0; i < array_len(imp->specs); i++) {
      WeakRef_Release(imp->specs[i]);
    }
    array_free(imp->specs);
  }
  rm_free(imp->slots);
  rm_free(imp->nameForLogs);
  rm_free(imp->taskId);
  rm_free(imp);
}

static size_t findIndex(const char *taskId) {
  if (!imports || !taskId) return SIZE_MAX;
  for (size_t i = 0; i < array_len(imports); i++) {
    if (!strcmp(imports[i]->taskId, taskId)) return i;
  }
  return SIZE_MAX;
}

static DiskASMImport *find(const char *taskId) {
  size_t i = findIndex(taskId);
  return i == SIZE_MAX ? NULL : imports[i];
}

// Flex ends the attempt with IMPORT_FAILED and does not retry it.
static void abortAttempt(DiskASMImport *imp, const char *error) {
  if (!RedisModule_ClusterAbortSlotMigration) {
    RedisModule_Log(RSDummyContext, "warning",
                    "ASM import %s: the server cannot abort the import; it stays blocked on the "
                    "import gate, or completes with the slots unindexed without it",
                    imp->taskId);
    return;
  }
  if (RedisModule_ClusterAbortSlotMigration(RSDummyContext, imp->taskId, error) == REDISMODULE_OK) {
    imp->stats.abortRequested = true;
    return;
  }
  // EALREADY: an abort is already scheduled. ENOENT: the attempt is gone, and its IMPORT_FAILED
  // tears this coordinator down.
  if (errno == EALREADY || errno == ENOENT) {
    return;
  }
  RedisModule_Log(RSDummyContext, "warning", "ASM import %s: aborting the import failed: %s",
                  imp->taskId, strerror(errno));
}

static void fail(DiskASMImport *imp, const char *error) {
  imp->stats.phase = DISK_ASM_IMPORT_REBUILD_FAILED;
  imp->stats.lastError = error;
  RedisModule_Log(RSDummyContext, "warning",
                  "ASM import %s: search cannot rebuild the imported slots, aborting it: %s",
                  imp->taskId, error);
  abortAttempt(imp, error);
}

// Takes `imp` out of the registry and frees it, or leaves it to its running sweep.
static void detach(DiskASMImport *imp, DiskASMImportPhase endPhase) {
  size_t i = findIndex(imp->taskId);
  RS_ASSERT(i != SIZE_MAX);
  array_del(imports, i);
  lastEnded = imp->stats;
  lastEnded.phase = endPhase;
  lastEnded.gateHeld = false;
  anyEnded = true;
  if (imp->scanRunning) {
    __atomic_store_n(&imp->ended, true, __ATOMIC_RELAXED);
  } else {
    importFree(imp);
  }
}

//---------------------------------------------------------------------------------------------
// The sweep

static bool importShouldStop(AsyncScanJob *job) {
  DiskASMImport *imp = (DiskASMImport *)job;
  return __atomic_load_n(&imp->ended, __ATOMIC_RELAXED);
}

static RedisModuleAsyncScanResult importStart(AsyncScanJob *job, RedisModuleCtx *ctx,
                                              RedisModuleScanCursor *cursor,
                                              RedisModuleAsyncScanKeyCB key_cb,
                                              RedisModuleAsyncScanCallDoneCB done_cb,
                                              size_t batchSizeHint, void *privdata) {
  DiskASMImport *imp = (DiskASMImport *)job;
  // Expired keys are delivered too: their DocIdMeta must be cleared, or a later expiry or a
  // delete of the live tail would unlink the source's docIds.
  const int openMode = REDISMODULE_OPEN_KEY_ACCESS_EXPIRED | REDISMODULE_OPEN_KEY_NOEXPIRE;
  // No filter: every key's DocIdMeta must be cleared, whether an index selects it or not.
  return RedisModule_AsyncScanStartInSlots(ctx, cursor, imp->slots, /*include_importing=*/1, NULL,
                                           REDISMODULE_ASYNCSCAN_MODE_META_AND_VALUE, &openMode,
                                           key_cb, done_cb, batchSizeHint, privdata);
}

// The sweep delivers each key at most once, so the clear never drops a docId this sweep wrote,
// and Indexes_AsyncScanIndexKey never meets a foreign docId.
static bool importOnKey(AsyncScanJob *job, RedisModuleCtx *ctx, RedisModuleKey *key,
                        RedisModuleString *name) {
  DiskASMImport *imp = (DiskASMImport *)job;
  if (importShouldStop(job)) {
    return false;
  }
  imp->stats.keysScanned++;
  if (DocIdMeta_ClearWithOpenKey(key) != REDISMODULE_OK) {
    imp->clearFailed = true;
    return false;
  }
  if (RedisModule_IsKeyExpired(key)) {
    return true;
  }
  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return true;
  }
  bool indexed = false;
  for (size_t i = 0; i < array_len(imp->specs); i++) {
    StrongRef ref = IndexSpecRef_Promote(imp->specs[i]);
    IndexSpec *sp = StrongRef_Get(ref);
    if (!sp) {
      continue;
    }
    RS_ASSERT(sp->specId <= imp->frozenMaxSpecId);
    indexed |= Indexes_AsyncScanIndexKey(sp, ctx, key, name, type);
    IndexSpecRef_Release(ref);
  }
  if (indexed) {
    imp->stats.keysIndexed++;
  }
  return true;
}

static void importFinish(AsyncScanJob *job, RedisModuleCtx *ctx, const AsyncScanEnd *end) {
  DiskASMImport *imp = (DiskASMImport *)job;
  imp->scanRunning = false;
  if (imp->ended) {
    importFree(imp);
    return;
  }
  RS_ASSERT(imp->stats.phase == DISK_ASM_IMPORT_SCANNING);
  if (imp->clearFailed) {
    fail(imp, "clearing the DocIdMeta of an imported key failed");
    return;
  }
  if (end->outcome != ASYNC_SCAN_OUTCOME_COMPLETED) {
    static const char *errors[] = {
        [ASYNC_SCAN_OUTCOME_CANCELLED] = "the rebuild scan was cancelled",
        [ASYNC_SCAN_OUTCOME_RESET] = "a dataset reset ended the rebuild scan",
        [ASYNC_SCAN_OUTCOME_OOM] = "the rebuild scan ran out of memory",
        [ASYNC_SCAN_OUTCOME_IO_ERROR] = "the rebuild scan hit an I/O error",
        [ASYNC_SCAN_OUTCOME_ERROR] = "the rebuild scan could not run",
    };
    fail(imp, errors[end->outcome]);
    return;
  }

  // Leave the window first: the gate release lets the live tail in, whose writes must be indexed
  // and whose deletes must find the docIds the sweep wrote.
  ASM_StateMachine_FinishImportRebuild(imp->slots);
  imp->stats.phase = DISK_ASM_IMPORT_REBUILT;
  imp->stats.gateHeld = false;
  RedisModule_Log(ctx, "notice",
                  "ASM import %s: rebuilt the imported slots (scanned=%zu, indexed=%zu)",
                  imp->taskId, imp->stats.keysScanned, imp->stats.keysIndexed);
  if (RedisModule_ClusterReleaseImportGate(ctx, imp->taskId) != REDISMODULE_OK) {
    // ENOENT: the attempt is gone, and its IMPORT_FAILED tears this coordinator down.
    imp->stats.lastError = "releasing the import gate failed";
    RedisModule_Log(ctx, "warning", "ASM import %s: releasing the import gate failed: %s",
                    imp->taskId, strerror(errno));
  }
}

static void importScanTask(void *arg) {
  AsyncScan_Drive(&((DiskASMImport *)arg)->job);
}

//---------------------------------------------------------------------------------------------
// Cleanup of failed imports

#define DISK_ASM_NUM_SLOTS 16384

// Slots of failed imports whose documents are not scheduled for deletion yet, allocated while
// there is one. Kept apart from the attempts: a failed attempt ends at IMPORT_FAILED, its cleanup
// only once Flex trims its keys.
static bool *cleanupPending = NULL;
static size_t numCleanupPending = 0;

static void markCleanupPending(const RedisModuleSlotRangeArray *slots) {
  if (!cleanupPending) cleanupPending = rm_calloc(DISK_ASM_NUM_SLOTS, sizeof(*cleanupPending));
  for (int32_t r = 0; r < slots->num_ranges; r++) {
    for (unsigned int slot = slots->ranges[r].start; slot <= slots->ranges[r].end; slot++) {
      if (!cleanupPending[slot]) {
        cleanupPending[slot] = true;
        numCleanupPending++;
      }
    }
  }
}

// Removes the slots of `slots` that await cleanup from the pending set and returns them, or NULL
// if there are none.
static RedisModuleSlotRangeArray *takeCleanupPending(const RedisModuleSlotRangeArray *slots) {
  if (!numCleanupPending || !slots) return NULL;
  size_t maxRanges = 0;
  for (int32_t r = 0; r < slots->num_ranges; r++) {
    maxRanges += slots->ranges[r].end - slots->ranges[r].start + 1;
  }
  RedisModuleSlotRangeArray *taken =
      rm_malloc(sizeof(*taken) + maxRanges * sizeof(taken->ranges[0]));
  taken->num_ranges = 0;
  for (int32_t r = 0; r < slots->num_ranges; r++) {
    for (unsigned int slot = slots->ranges[r].start; slot <= slots->ranges[r].end; slot++) {
      if (!cleanupPending[slot]) continue;
      cleanupPending[slot] = false;
      numCleanupPending--;
      RedisModuleSlotRange *last = taken->num_ranges ? &taken->ranges[taken->num_ranges - 1] : NULL;
      if (last && last->end + 1 == slot) {
        last->end = slot;
      } else {
        taken->ranges[taken->num_ranges++] = (RedisModuleSlotRange){slot, slot};
      }
    }
  }
  if (!numCleanupPending) {
    rm_free(cleanupPending);
    cleanupPending = NULL;
  }
  if (!taken->num_ranges) {
    rm_free(taken);
    return NULL;
  }
  return taken;
}

static void cleanUpFailedImport(const RedisModuleSlotRangeArray *slots, bool docsDeleted,
                                const char *trigger) {
  RedisModuleSlotRangeArray *pending = takeCleanupPending(slots);
  if (!pending) return;
  RedisModule_Log(RSDummyContext, "notice",
                  "ASM import: deleting the documents of a failed import's slots on %s", trigger);
  // The deletion only covers documents that exist now, so a later import of the slots keeps its
  // own.
  if (!docsDeleted) Indexes_DeleteSlots(pending);
  ASM_StateMachine_FinishFailedImportCleanup(pending);
  rm_free(pending);
}

void DiskASM_ImportRebuild_OnSlotsTrimmed(const RedisModuleSlotRangeArray *slots,
                                          bool docsDeleted) {
  cleanUpFailedImport(slots, docsDeleted, "their trim");
}

void DiskASM_ImportRebuild_OnImportStarted(const RedisModuleSlotRangeArray *slots) {
  // Flex starts no import of slots it is still trimming, so their trim ended without an event.
  cleanUpFailedImport(slots, false, "a new import of them");
}

//---------------------------------------------------------------------------------------------
// Stage events

void DiskASM_ImportRebuild_OnBaseReady(RedisModuleCtx *ctx,
                                       const RedisModuleClusterSlotMigrationInfo *info) {
  DiskASMImport *stale = find(info->task_id);
  if (stale) {
    // Flex fires the stages once per attempt and ends an attempt before retrying it.
    RS_LOG_ASSERT(false, "ASM import: a second IMPORT_SST_BASE_READY for one attempt");
    detach(stale, DISK_ASM_IMPORT_FAILED);
  }

  DiskASMImport *imp = rm_calloc(1, sizeof(*imp));
  imp->taskId = rm_strdup(info->task_id);
  rm_asprintf(&imp->nameForLogs, "ASM import %s", imp->taskId);
  imp->slots = copySlots(info->slots);
  snprintf(imp->stats.taskId, sizeof(imp->stats.taskId), "%s", imp->taskId);
  imp->stats.phase = DISK_ASM_IMPORT_AWAITING_OVERLAY_END;
  imp->job = (AsyncScanJob){
      .nameForLogs = imp->nameForLogs,
      .scannedKeys = &imp->stats.keysScanned,
      .shouldStop = importShouldStop,
      .isCancelled = importShouldStop,
      .start = importStart,
      .onKey = importOnKey,
      .finish = importFinish,
  };
  if (!imports) imports = array_new(DiskASMImport *, 1);
  array_append(imports, imp);

  if (RedisModule_ClusterAcquireImportGate(ctx, imp->taskId) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "ASM import %s: acquiring the import gate failed: %s",
                    imp->taskId, strerror(errno));
    fail(imp, "acquiring the import gate failed");
    return;
  }
  imp->stats.gateHeld = true;
  RedisModule_Log(ctx, "notice", "ASM import %s: holding the import gate", imp->taskId);
}

static void freezeSpec(IndexSpec *sp, void *ud) {
  DiskASMImport *imp = ud;
  array_append(imp->specs, StrongRef_Demote(IndexSpec_GetStrongRefUnsafe(sp)));
  if (sp->specId > imp->frozenMaxSpecId) imp->frozenMaxSpecId = sp->specId;
}

void DiskASM_ImportRebuild_OnOverlayEnd(RedisModuleCtx *ctx,
                                        const RedisModuleClusterSlotMigrationInfo *info) {
  DiskASMImport *imp = find(info->task_id);
  if (!imp) {
    RedisModule_Log(ctx, "warning", "ASM import %s: overlay ended for an unknown attempt; ignored",
                    info->task_id);
    return;
  }
  if (imp->stats.phase != DISK_ASM_IMPORT_AWAITING_OVERLAY_END) {
    RS_LOG_ASSERT(imp->stats.phase == DISK_ASM_IMPORT_REBUILD_FAILED,
                  "ASM import: the overlay ended twice");
    return;
  }
  // FT._RESTOREIFNX in the overlay starts no backfill, so these are all the indexes the imported
  // keys belong to.
  imp->specs = array_new(WeakRef, 1);
  Indexes_ForEachSpec(freezeSpec, imp);

  imp->stats.phase = DISK_ASM_IMPORT_SCANNING;
  imp->scanRunning = true;
  RedisModule_Log(ctx, "notice",
                  "ASM import %s: rebuilding the imported slots for %u indexes (max specId %lu)",
                  imp->taskId, array_len(imp->specs), (unsigned long)imp->frozenMaxSpecId);
  ReindexPool_AddImportTask(importScanTask, imp);
}

bool DiskASM_ImportRebuild_OnImportEnded(const RedisModuleClusterSlotMigrationInfo *info,
                                         bool completed) {
  DiskASMImport *imp = find(info->task_id);
  if (!imp) return false;
  if (completed) {
    // The gate is held until the rebuild completed, and Flex completes no import before.
    RS_LOG_ASSERT(imp->stats.phase == DISK_ASM_IMPORT_REBUILT,
                  "ASM import: completed before search rebuilt its slots");
  } else {
    // The keys are trimmed with the source's docIds, or with the sweep's, and the window keeps
    // their unlinks from deleting a document by them. The window also covers an attempt that
    // failed before its overlay started.
    ASM_StateMachine_AwaitFailedImportCleanup(imp->slots);
    markCleanupPending(imp->slots);
  }
  detach(imp, completed ? DISK_ASM_IMPORT_COMPLETED : DISK_ASM_IMPORT_FAILED);
  return !completed;
}

//---------------------------------------------------------------------------------------------
// Window checks and INFO

#ifdef ENABLE_ASSERT
static bool slotsContain(const RedisModuleSlotRangeArray *slots, unsigned int slot) {
  for (int32_t i = 0; i < slots->num_ranges; i++) {
    if (slot >= slots->ranges[i].start && slot <= slots->ranges[i].end) return true;
  }
  return false;
}

bool DiskASM_ImportRebuild_IsRebuildingKey(const RedisModuleString *key) {
  if (!imports || !key) return false;
  size_t len;
  const char *name = RedisModule_StringPtrLen(key, &len);
  unsigned int slot = RedisModule_ClusterKeySlotC(name, len);
  for (size_t i = 0; i < array_len(imports); i++) {
    DiskASMImport *imp = imports[i];
    if (imp->stats.phase == DISK_ASM_IMPORT_SCANNING && slotsContain(imp->slots, slot)) {
      return true;
    }
  }
  return false;
}
#endif

void DiskASM_ImportRebuild_AddInfo(RedisModuleInfoCtx *ctx) {
  const size_t active = imports ? array_len(imports) : 0;
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_imports_active", active);
  const DiskASMImportStats *stats = NULL;
  if (active) {
    stats = &imports[active - 1]->stats;
  } else if (anyEnded) {
    stats = &lastEnded;
  }
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_task_id", stats ? stats->taskId : "");
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_phase",
                                  stats ? phaseName(stats->phase) : "none");
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_gate_held", stats ? stats->gateHeld : 0);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_abort_requested",
                                    stats ? stats->abortRequested : 0);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_cleanup_pending_slots",
                                    numCleanupPending);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_keys_scanned",
                                    stats ? stats->keysScanned : 0);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_keys_indexed",
                                    stats ? stats->keysIndexed : 0);
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_last_error",
                                  stats && stats->lastError ? stats->lastError : "none");
}
