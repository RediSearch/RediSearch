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
#include "spec.h"
#include "util/arr/arr.h"
#include "util/references.h"

typedef enum {
  // The gate is held; waiting for the overlay to end.
  DISK_ASM_IMPORT_AWAITING_OVERLAY_END,
  // The sweep is queued or running, and owns the DiskASMImport until its finish.
  DISK_ASM_IMPORT_SCANNING,
  // The slots are rebuilt and the gate released.
  DISK_ASM_IMPORT_REBUILT,
  // The rebuild cannot complete; the gate, if held, is kept so the import cannot finish.
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
  bool gateHeld;
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
  DiskASMImportPhase phase;
  DiskASMImportStats stats;
  // The participating indexes, frozen when the overlay ends. All have a specId of at most
  // frozenMaxSpecId.
  arrayof(WeakRef) specs;
  uint64_t frozenMaxSpecId;
  // The attempt ended while the sweep ran: the sweep stops and frees the import. Read by the sweep
  // without the GIL.
  bool ended;
} DiskASMImport;

// Flex runs at most one ASM task per node, and ends an attempt before retrying it. An ended
// attempt whose sweep still runs is owned by that sweep, not by this.
static DiskASMImport *activeImport = NULL;
// The latest attempt that ended, for INFO once no import is active.
static DiskASMImportStats lastEnded;
static DiskASMImportPhase lastEndedPhase;
static bool anyEnded = false;

static RedisModuleSlotRangeArray *copySlots(const RedisModuleSlotRangeArray *slots) {
  size_t size = sizeof(*slots) + slots->num_ranges * sizeof(slots->ranges[0]);
  RedisModuleSlotRangeArray *copy = rm_malloc(size);
  memcpy(copy, slots, size);
  return copy;
}

static void importFree(DiskASMImport *imp) {
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

static void fail(DiskASMImport *imp, const char *error) {
  imp->phase = DISK_ASM_IMPORT_REBUILD_FAILED;
  imp->stats.lastError = error;
  // TODO(MOD-20202): abort the attempt with RedisModule_ClusterAbortSlotMigration; until then it
  // stays blocked on the gate, or, without the gate, completes with the slots unindexed.
  RedisModule_Log(RSDummyContext, "warning",
                  "ASM import %s: search cannot rebuild the imported slots: %s", imp->taskId,
                  error);
}

// Clears `activeImport` and frees it, or leaves it to its running sweep.
static void detach(DiskASMImportPhase endPhase) {
  DiskASMImport *imp = activeImport;
  activeImport = NULL;
  lastEnded = imp->stats;
  lastEndedPhase = endPhase;
  lastEnded.gateHeld = false;
  anyEnded = true;
  if (imp->phase == DISK_ASM_IMPORT_SCANNING) {
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
    // The slots cannot leave the window; the finish fails the rebuild with this error.
    imp->stats.lastError = "clearing the DocIdMeta of an imported key failed";
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
  RS_ASSERT(imp->phase == DISK_ASM_IMPORT_SCANNING);
  if (imp->ended) {
    importFree(imp);
    return;
  }
  if (imp->stats.lastError) {
    fail(imp, imp->stats.lastError);
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
  imp->phase = DISK_ASM_IMPORT_REBUILT;
  imp->stats.gateHeld = false;
  RedisModule_Log(ctx, "notice",
                  "ASM import %s: rebuilt the imported slots (scanned=%zu, indexed=%zu)",
                  imp->taskId, imp->stats.keysScanned, imp->stats.keysIndexed);
  if (RedisModule_ClusterReleaseImportGate(ctx, imp->taskId) != REDISMODULE_OK) {
    // ENOENT: the attempt is gone, and its IMPORT_FAILED ends this import.
    imp->stats.lastError = "releasing the import gate failed";
    RedisModule_Log(ctx, "warning", "ASM import %s: releasing the import gate failed: %s",
                    imp->taskId, strerror(errno));
  }
}

static void importScanTask(void *arg) {
  AsyncScan_Drive(&((DiskASMImport *)arg)->job);
}

//---------------------------------------------------------------------------------------------
// Stage events

void DiskASM_ImportRebuild_OnBaseReady(RedisModuleCtx *ctx,
                                       const RedisModuleClusterSlotMigrationInfo *info) {
  if (activeImport) {
    RS_LOG_ASSERT(false, "ASM import: IMPORT_SST_BASE_READY while another attempt is live");
    detach(DISK_ASM_IMPORT_FAILED);
  }

  DiskASMImport *imp = rm_calloc(1, sizeof(*imp));
  imp->taskId = rm_strdup(info->task_id);
  rm_asprintf(&imp->nameForLogs, "ASM import %s", imp->taskId);
  imp->slots = copySlots(info->slots);
  snprintf(imp->stats.taskId, sizeof(imp->stats.taskId), "%s", imp->taskId);
  imp->phase = DISK_ASM_IMPORT_AWAITING_OVERLAY_END;
  imp->job = (AsyncScanJob){
      .nameForLogs = imp->nameForLogs,
      .scannedKeys = &imp->stats.keysScanned,
      .shouldStop = importShouldStop,
      .isCancelled = importShouldStop,
      .start = importStart,
      .onKey = importOnKey,
      .finish = importFinish,
  };
  activeImport = imp;

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

void DiskASM_ImportRebuild_OnOverlayEnd(RedisModuleCtx *ctx) {
  DiskASMImport *imp = activeImport;
  if (!imp) {
    // Base-ready precedes the overlay and creates the import even when the gate is refused.
    RS_LOG_ASSERT(false, "ASM import: the overlay ended before the base was ready");
    return;
  }
  if (imp->phase != DISK_ASM_IMPORT_AWAITING_OVERLAY_END) {
    RS_LOG_ASSERT(imp->phase == DISK_ASM_IMPORT_REBUILD_FAILED,
                  "ASM import: the overlay ended twice");
    return;
  }
  // FT._RESTOREIFNX in the overlay starts no backfill, so these are all the indexes the imported
  // keys belong to.
  imp->specs = array_new(WeakRef, 1);
  Indexes_ForEachSpec(freezeSpec, imp);

  imp->phase = DISK_ASM_IMPORT_SCANNING;
  RedisModule_Log(ctx, "notice",
                  "ASM import %s: rebuilding the imported slots for %u indexes (max specId %lu)",
                  imp->taskId, array_len(imp->specs), (unsigned long)imp->frozenMaxSpecId);
  ReindexPool_AddImportTask(importScanTask, imp);
}

void DiskASM_ImportRebuild_OnImportEnded(bool completed) {
  // Only an SST import creates a DiskASMImport: a logical import, or one that failed before its
  // base was ready, ends with none.
  if (!activeImport) return;
  if (completed) {
    // The gate is held until the rebuild completed, and Flex completes no import before.
    RS_LOG_ASSERT(activeImport->phase == DISK_ASM_IMPORT_REBUILT,
                  "ASM import: completed before search rebuilt its slots");
  }
  detach(completed ? DISK_ASM_IMPORT_COMPLETED : DISK_ASM_IMPORT_FAILED);
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
  if (!activeImport || activeImport->phase != DISK_ASM_IMPORT_SCANNING || !key) return false;
  size_t len;
  const char *name = RedisModule_StringPtrLen(key, &len);
  return slotsContain(activeImport->slots, RedisModule_ClusterKeySlotC(name, len));
}
#endif

void DiskASM_ImportRebuild_AddInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_imports_active", activeImport != NULL);
  const DiskASMImportStats *stats = NULL;
  const char *phase = "none";
  if (activeImport) {
    stats = &activeImport->stats;
    phase = phaseName(activeImport->phase);
  } else if (anyEnded) {
    stats = &lastEnded;
    phase = phaseName(lastEndedPhase);
  }
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_task_id", stats ? stats->taskId : "");
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_phase", phase);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_gate_held", stats ? stats->gateHeld : 0);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_keys_scanned",
                                    stats ? stats->keysScanned : 0);
  RedisModule_InfoAddFieldULongLong(ctx, "disk_asm_import_keys_indexed",
                                    stats ? stats->keysIndexed : 0);
  RedisModule_InfoAddFieldCString(ctx, "disk_asm_import_last_error",
                                  stats && stats->lastError ? stats->lastError : "none");
}
