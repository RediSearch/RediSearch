/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec_scanner.h"
#include "spec.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "doc_types.h"
#include "document.h"
#include "rules.h"
#include "info/index_error.h"
#include "debug_commands.h"
#include "util/workers.h"
#include "util/logging.h"
#include "util/redis_mem_info.h"
#include "indexer.h"
#include "util/dict.h"

#include <unistd.h>

extern RedisModuleCtx *RSDummyContext;
extern DebugCTX globalDebugCtx;
extern dict *legacySpecDict;
extern dict *legacySpecRules;
extern dict *specDict_g;

// Defined in spec.c — called from Indexes_EndRDBLoadingEvent
void Indexes_UpgradeLegacyIndexes(void);

// Default values make no limits.
extern size_t memoryLimit;
extern size_t used_memory;

IndexesScanner *global_spec_scanner = NULL;

const char *DEBUG_INDEX_SCANNER_STATUS_STRS[] = {
    "NEW", "SCANNING", "DONE", "CANCELLED", "PAUSED", "RESUMED", "PAUSED_ON_OOM", "PAUSED_BEFORE_OOM_RETRY",
};

// Static assertion to ensure array size matches the number of statuses
static_assert(
    (sizeof(DEBUG_INDEX_SCANNER_STATUS_STRS) / sizeof(char*)) == DEBUG_INDEX_SCANNER_CODE_COUNT,
    "Mismatch between DebugIndexScannerCode enum and DEBUG_INDEX_SCANNER_STATUS_STRS array"
);

// Forward declarations for static functions
static DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref);
static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             DebugIndexesScanner *dScanner);
static void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code);
static IndexesScanner *IndexesScanner_New(StrongRef global_ref);
static IndexesScanner *IndexesScanner_NewGlobal(void);
static void Indexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             IndexesScanner *scanner);
static void Indexes_ScanAndReindexTask(IndexesScanner *scanner);
static void IndexSpec_ScanAndReindexAsync(StrongRef spec_ref);

static redisearch_thpool_t *reindexPool = NULL;

//---------------------------------------------------------------------------------------------
// Memory/OOM helpers
//---------------------------------------------------------------------------------------------

void setMemoryInfo(RedisModuleCtx *ctx) {
  #define MIN_NOT_0(a,b) (((a)&&(b))?MIN((a),(b)):MAX((a),(b)))
  RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "memory");

  size_t maxmemory = RedisModule_ServerInfoGetFieldUnsigned(info, "maxmemory", NULL);
  size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL); // Enterprise limit
  maxmemory = MIN_NOT_0(maxmemory, max_process_mem);

  size_t total_system_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "total_system_memory", NULL);
  memoryLimit = MIN_NOT_0(maxmemory, total_system_memory);

  used_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);

  RedisModule_FreeServerInfo(ctx, info);
}

// Return true if used_memory exceeds (indexingMemoryLimit % × memoryLimit); false if within bounds or limit is 0.
static inline bool isBgIndexingMemoryOverLimit(RedisModuleCtx *ctx) {
  // if memory limit is set to 0, we don't need to check for memory usage
  if(RSGlobalConfig.indexingMemoryLimit == 0) {
    return false;
  }

  float used_memory_ratio = RedisMemory_GetUsedMemoryRatioUnified(ctx);
  float memory_limit_ratio = (float)RSGlobalConfig.indexingMemoryLimit / 100;

  return (used_memory_ratio > memory_limit_ratio) ;
}

// This function should be called after the first background scan OOM error
// It will wait for resource manager to allocate more memory to the process if possible
// and after the function returns, the scan will continue
static inline void threadSleepByConfigTime(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  // Thread sleep based on the config
  uint32_t sleepTime = RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry;
  RedisModule_Log(ctx, "notice", "Scanning index %s in background: paused for %u seconds due to OOM, waiting for memory allocation",
                  scanner->spec_name_for_logs, sleepTime);

  RedisModule_ThreadSafeContextUnlock(ctx);
  sleep(sleepTime);
  RedisModule_ThreadSafeContextLock(ctx);
}

// This function should be called after the second background scan OOM error
// It will stop the background scan process
static inline void scanStopAfterOOM(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  char* error;
  rm_asprintf(&error, "Used memory is more than %u percent of max memory, cancelling the scan", RSGlobalConfig.indexingMemoryLimit);
  RedisModule_Log(ctx, "warning", "%s", error);

    // We need to report the error message besides the log, so we can show it in FT.INFO
  if(!scanner->global) {
    scanner->cancelled = true;
    StrongRef curr_run_ref = WeakRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(curr_run_ref);
    if (sp) {
      sp->scan_failed_OOM = true;
      // Error message does not contain user data
      IndexError_AddError(&sp->stats.indexError, error, error, scanner->OOMkey);
      IndexError_RaiseBackgroundIndexFailureFlag(&sp->stats.indexError);
      StrongRef_Release(curr_run_ref);
    } else {
      // spec was deleted
      RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled due to OOM and index was dropped",
                    scanner->spec_name_for_logs);
      }
    }
    rm_free(error);
}

//---------------------------------------------------------------------------------------------
// Scanner lifecycle
//---------------------------------------------------------------------------------------------

static IndexesScanner *IndexesScanner_NewGlobal() {
  if (global_spec_scanner) {
    return NULL;
  }

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->global = true;
  scanner->scannedKeys = 0;

  global_spec_scanner = scanner;
  RedisModule_Log(RSDummyContext, "notice", "Global scanner created");

  return scanner;
}

static IndexesScanner *IndexesScanner_New(StrongRef global_ref) {

  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));

  scanner->spec_ref = StrongRef_Demote(global_ref);
  IndexSpec *spec = StrongRef_Get(global_ref);
  scanner->spec_name_for_logs = rm_strdup(IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog));
  scanner->isDebug = false;

  // scan already in progress?
  if (spec->scanner) {
    // cancel ongoing scan, keep on_progress indicator on
    IndexesScanner_Cancel(spec->scanner);
    const char* name = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
    RedisModule_Log(RSDummyContext, "notice", "Scanning index %s in background: cancelled and restarted", name);
  }
  spec->scanner = scanner;
  spec->scan_in_progress = true;

  return scanner;
}

void IndexesScanner_Free(IndexesScanner *scanner) {
  rm_free(scanner->spec_name_for_logs);
  if (global_spec_scanner == scanner) {
    global_spec_scanner = NULL;
  } else {
    StrongRef tmp = WeakRef_Promote(scanner->spec_ref);
    IndexSpec *spec = StrongRef_Get(tmp);
    if (spec) {
      if (spec->scanner == scanner) {
        spec->scanner = NULL;
        spec->scan_in_progress = false;
      }
      StrongRef_Release(tmp);
    }
    WeakRef_Release(scanner->spec_ref);
  }
  // Free the last scanned key
  if (scanner->OOMkey) {
    RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
  }
  rm_free(scanner);
}

void IndexesScanner_Cancel(IndexesScanner *scanner) {
  scanner->cancelled = true;
}

void IndexesScanner_ResetProgression(IndexesScanner *scanner) {
  scanner-> scanFailedOnOOM = false;
  scanner-> scannedKeys = 0;
}

//---------------------------------------------------------------------------------------------
// Scan execution
//---------------------------------------------------------------------------------------------

static void IndexSpec_DoneIndexingCallabck(struct RSAddDocumentCtx *docCtx, RedisModuleCtx *ctx,
                                           void *pd) {
}

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type);
static void Indexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             IndexesScanner *scanner) {

  if (scanner->cancelled) {
    return;
  }

  if (isBgIndexingMemoryOverLimit(ctx)){
    scanner->scanFailedOnOOM = true;
    if (scanner->OOMkey) {
      RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
    }
    // Hold the key that triggered OOM in case we need to attach an index error
    scanner->OOMkey = RedisModule_HoldString(RSDummyContext, keyname);
    return;
  }
  // RMKey it is provided as best effort but in some cases it might be NULL
  bool keyOpened = false;
  if (!key || isCrdt) {
    key = RedisModule_OpenKey(ctx, keyname, DOCUMENT_OPEN_KEY_INDEXING_FLAGS);
    keyOpened = true;
  }
  // Get the document type
  DocumentType type = getDocType(key);

  // Close the key if we opened it
  if (keyOpened) {
    RedisModule_CloseKey(key);
  }

  // Verify that the document type is supported and document is not empty
  if (type == DocumentType_Unsupported) {
    return;
  }

  if (scanner->global) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, keyname, type, NULL);
  } else {
    StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(curr_run_ref);
    if (sp) {
      // This check is performed without locking the spec, but it's ok since we locked the GIL
      // So the main thread is not running and the GC is not touching the relevant data
      if (SchemaRule_ShouldIndex(sp, keyname, type)) {
        IndexSpec_UpdateDoc(sp, ctx, keyname, type);
      }
      IndexSpecRef_Release(curr_run_ref);
    } else {
      // spec was deleted, cancel scan
      scanner->cancelled = true;
    }
  }
  ++scanner->scannedKeys;
}

//---------------------------------------------------------------------------------------------
// Define for neater code, first argument is the debug scanner flag field , second is the status code
#define IF_DEBUG_PAUSE_CHECK(scanner, ctx, status_bool, status_code) \
if (scanner->isDebug) { \
  DebugIndexesScanner *dScanner = (DebugIndexesScanner*)scanner;\
  DebugIndexesScanner_pauseCheck(dScanner, ctx, dScanner->status_bool, status_code); \
}
#define IF_DEBUG_PAUSE_CHECK_BEFORE_OOM_RETRY(scanner, ctx) IF_DEBUG_PAUSE_CHECK(scanner, ctx, pauseBeforeOOMRetry, DEBUG_INDEX_SCANNER_CODE_PAUSED_BEFORE_OOM_RETRY)
#define IF_DEBUG_PAUSE_CHECK_ON_OOM(scanner, ctx) IF_DEBUG_PAUSE_CHECK(scanner, ctx, pauseOnOOM, DEBUG_INDEX_SCANNER_CODE_PAUSED_ON_OOM)

static void Indexes_ScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  size_t counter = 0;
  RedisModuleScanCB scanner_func = (RedisModuleScanCB)Indexes_ScanProc;

  RedisModuleCtx *ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_ThreadSafeContextLock(ctx);

  if (scanner->cancelled) {
    goto end;
  }
  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background");
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", scanner->spec_name_for_logs);
  }
  if (globalDebugCtx.debugMode) {
    // If we are in debug mode, we need to use the debug scanner function
    scanner_func = (RedisModuleScanCB)DebugIndexes_ScanProc;

    // If background indexing paused, wait until it is resumed
    // Allow the redis server to acquire the GIL while we release it
    RedisModule_ThreadSafeContextUnlock(ctx);
    while (globalDebugCtx.bgIndexing.pause) { // volatile variable
      usleep(1000);
    }
    RedisModule_ThreadSafeContextLock(ctx);
  }

  while (RedisModule_Scan(ctx, cursor, scanner_func, scanner)) {
    RedisModule_ThreadSafeContextUnlock(ctx);
    counter++;
    if (counter % RSGlobalConfig.numBGIndexingIterationsBeforeSleep == 0) {
      // Sleep to allow redis server to acquire the GIL while we release it.
      // We do that periodically every X iterations (100 as default), otherwise we call
      // 'sched_yield()'. That is since 'sched_yield()' doesn't give up the processor for enough
      // time to ensure that other threads that are waiting for the GIL will actually have the
      // chance to take it.
      usleep(RSGlobalConfig.bgIndexingSleepDurationMicroseconds);
      IncrementBgIndexYieldCounter();
    } else {
      sched_yield();
    }
    RedisModule_ThreadSafeContextLock(ctx);

    // Check if we need to handle OOM but must check if the scanner was cancelled for other reasons (i.e. FT. ALTER)
    if (scanner->scanFailedOnOOM && !scanner->cancelled) {

      // Check the config to see if we should wait for memory allocation
      if(RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry > 0) {
        IF_DEBUG_PAUSE_CHECK_BEFORE_OOM_RETRY(scanner, ctx);
        // Call the wait function
        threadSleepByConfigTime(ctx, scanner);
        if (!isBgIndexingMemoryOverLimit(ctx)) {
          // We can continue the scan
          RedisModule_Log(ctx, "notice", "Scanning index %s in background: resuming after OOM due to memory limit increase",
                          scanner->spec_name_for_logs);
          IndexesScanner_ResetProgression(scanner);
          RedisModule_ScanCursorRestart(cursor);
          continue;
        }
      }
      // At this point we either waited for memory allocation and failed
      // or the config is set to not wait for memory allocation after OOM
      scanStopAfterOOM(ctx, scanner);
      IF_DEBUG_PAUSE_CHECK_ON_OOM(scanner, ctx);
    }

    if (scanner->cancelled) {

      if (scanner->global) {
        RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%ld)",
                        scanner->scannedKeys);
      } else {
        RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled (scanned=%ld)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
        goto end;
      }
    }
  }

  if (scanner->isDebug) {
    DebugIndexesScanner* dScanner = (DebugIndexesScanner*)scanner;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_DONE;
  }

  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)",
                    scanner->scannedKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%ld)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
  }

end:
  if (!scanner->cancelled && scanner->global) {
    Indexes_SetTempSpecsTimers(TimerOp_Add);
  }

  IndexesScanner_Free(scanner);

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindexAsync(StrongRef spec_ref) {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "reindex");
  }
#ifdef _DEBUG
  IndexSpec* spec = (IndexSpec*)StrongRef_Get(spec_ref);
  const char* indexName = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(RSDummyContext, "notice", "Register index %s for async scan", indexName);
#endif
  IndexesScanner *scanner;
  if (globalDebugCtx.debugMode) {
    // If we are in debug mode, we need to allocate a debug scanner
    scanner = (IndexesScanner*)DebugIndexesScanner_New(spec_ref);
    // If we need to pause before the scan, we set the pause flag
    if (globalDebugCtx.bgIndexing.pauseBeforeScan) {
      globalDebugCtx.bgIndexing.pause = true;
    }
  } else {
    scanner = IndexesScanner_New(spec_ref);
  }

  redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
}

void ReindexPool_ThreadPoolDestroy() {
  if (reindexPool != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(reindexPool);
    reindexPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

// Assumes that the spec is in a safe state to set a scanner on it (write lock or main thread)
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef spec_ref) {
  size_t nkeys = RedisModule_DbSize(ctx);
  if (nkeys > 0) {
    IndexSpec_ScanAndReindexAsync(spec_ref);
  }
}

void Indexes_ScanAndReindex() {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "reindex");
  }

  RedisModule_Log(RSDummyContext, "notice", "Scanning all indexes");
  IndexesScanner *scanner = IndexesScanner_NewGlobal();
  // check no global scan is in progress
  if (scanner) {
    redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
  }
}

//---------------------------------------------------------------------------------------------
// Debug Scanner Functions
//---------------------------------------------------------------------------------------------

static DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref) {

  DebugIndexesScanner *dScanner = rm_realloc(IndexesScanner_New(global_ref), sizeof(DebugIndexesScanner));

  dScanner->maxDocsTBscanned = globalDebugCtx.bgIndexing.maxDocsTBscanned;
  dScanner->maxDocsTBscannedPause = globalDebugCtx.bgIndexing.maxDocsTBscannedPause;
  dScanner->wasPaused = false;
  dScanner->status = DEBUG_INDEX_SCANNER_CODE_NEW;
  dScanner->base.isDebug = true;
  dScanner->pauseOnOOM = globalDebugCtx.bgIndexing.pauseOnOOM;
  dScanner->pauseBeforeOOMRetry = globalDebugCtx.bgIndexing.pauseBeforeOOMretry;

  IndexSpec *spec = StrongRef_Get(global_ref);
  spec->scanner = (IndexesScanner*)dScanner;

  return dScanner;
}

static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             DebugIndexesScanner *dScanner) {

  IndexesScanner *scanner = &(dScanner->base);

  if (dScanner->status == DEBUG_INDEX_SCANNER_CODE_NEW) {
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_RUNNING;
  }

  if (dScanner->maxDocsTBscannedPause > 0 && (!dScanner->wasPaused) && scanner->scannedKeys == dScanner->maxDocsTBscannedPause) {
    globalDebugCtx.bgIndexing.pause = true;
    dScanner->wasPaused = true;
  }

  if ((dScanner->maxDocsTBscanned > 0) && (scanner->scannedKeys == dScanner->maxDocsTBscanned)) {
    scanner->cancelled = true;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_CANCELLED;
  }

  // Check if we need to pause the scan before we release the GIL
  if (globalDebugCtx.bgIndexing.pause)
  {
      // Warning: This section is highly unsafe. RM_Scan does not permit the callback
      // function (i.e., this function) to release the GIL.
      // If the key currently being scanned is deleted after the GIL is released,
      // it can lead to a use-after-free and crash Redis.
      RedisModule_Log(ctx, "warning", "RM_Scan callback function is releasing the GIL, which is unsafe.");

      RedisModule_ThreadSafeContextUnlock(ctx);
      while (globalDebugCtx.bgIndexing.pause) { // volatile variable
        dScanner->status = DEBUG_INDEX_SCANNER_CODE_PAUSED;
        usleep(1000);
      }
      RedisModule_ThreadSafeContextLock(ctx);
  }

  if (dScanner->status == DEBUG_INDEX_SCANNER_CODE_PAUSED) {
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_RESUMED;
  }

  Indexes_ScanProc(ctx, keyname, key, &(dScanner->base));
}

// If this function is called, it means that the scan did not complete due to OOM, should be verified by the caller
static inline void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code) {
  if (!dScanner || !pauseField) {
    return;
  }
  globalDebugCtx.bgIndexing.pause = true;
  RedisModule_ThreadSafeContextUnlock(ctx);
  while (globalDebugCtx.bgIndexing.pause) { // volatile variable
    dScanner->status = code;
    usleep(1000);
  }
  dScanner->status = DEBUG_INDEX_SCANNER_CODE_RESUMED;
  RedisModule_ThreadSafeContextLock(ctx);
}

//---------------------------------------------------------------------------------------------
// RDB loading events
//---------------------------------------------------------------------------------------------

void Indexes_StartRDBLoadingEvent() {
  Indexes_Free(specDict_g, false);
  if (legacySpecDict) {
    dictEmpty(legacySpecDict, NULL);
  } else {
    legacySpecDict = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  }
  g_isLoading = true;
}

void Indexes_EndRDBLoadingEvent(RedisModuleCtx *ctx) {
  int hasLegacyIndexes = dictSize(legacySpecDict);
  Indexes_UpgradeLegacyIndexes();

  // we do not need the legacy dict specs anymore
  dictRelease(legacySpecDict);
  legacySpecDict = NULL;

  LegacySchemaRulesArgs_Free(ctx);

  if (hasLegacyIndexes) {
    Indexes_ScanAndReindex();
  }
}

void Indexes_EndLoading() {
  g_isLoading = false;
}
