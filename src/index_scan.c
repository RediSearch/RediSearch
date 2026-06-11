/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "index_scan.h"

#include <unistd.h>
#include <sched.h>

#include "config.h"
#include "debug_commands.h"
#include "thpool/thpool.h"
#include "util/logging.h"
#include "util/redis_mem_info.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

// Owned by debug_commands.c; consulted by the (debug) scanner here.
extern DebugCTX globalDebugCtx;
// Shared detached module context used for background work.
extern RedisModuleCtx *RSDummyContext;

const char *DEBUG_INDEX_SCANNER_STATUS_STRS[] = {
    "NEW", "SCANNING", "DONE", "CANCELLED", "PAUSED", "RESUMED", "PAUSED_ON_OOM", "PAUSED_BEFORE_OOM_RETRY",
};

// Static assertion to ensure array size matches the number of statuses
static_assert(
    (sizeof(DEBUG_INDEX_SCANNER_STATUS_STRS) / sizeof(char*)) == DEBUG_INDEX_SCANNER_CODE_COUNT,
    "Mismatch between DebugIndexScannerCode enum and DEBUG_INDEX_SCANNER_STATUS_STRS array"
);

static redisearch_thpool_t *reindexPool = NULL;

// Debug scanner functions
static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                                  DebugIndexesScanner *dScanner);
static void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code);

//---------------------------------------------------------------------------------------------

// This function should be called after the first background scan OOM error.
// It waits for the resource manager to allocate more memory if possible; after it
// returns the scan continues.
static inline void threadSleepByConfigTime(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  // Thread sleep based on the config
  uint32_t sleepTime = RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry;
  RedisModule_Log(ctx, "notice", "Scanning index %s in background: paused for %u seconds due to OOM, waiting for memory allocation",
                  scanner->name, sleepTime);

  RedisModule_ThreadSafeContextUnlock(ctx);
  sleep(sleepTime);
  RedisModule_ThreadSafeContextLock(ctx);
}

// Called after a (retried) background-scan OOM to stop the scan. Logs the failure and,
// for a single-target scan, marks it cancelled and lets the caller record the error
// via on_oom. A global (scan-all) scan has no single target, so it only logs.
static inline void scanStopAfterOOM(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  char* error;
  rm_asprintf(&error, "Used memory is more than %u percent of max memory, cancelling the scan", RSGlobalConfig.indexingMemoryLimit);
  RedisModule_Log(ctx, "warning", "%s", error);

  // We need to report the error message besides the log, so it shows in FT.INFO.
  if (!scanner->global) {
    scanner->cancelled = true;
    if (scanner->cbs.on_oom) {
      scanner->cbs.on_oom(scanner->privdata, ctx, error, scanner->OOMkey);
    }
  }
  rm_free(error);
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

//---------------------------------------------------------------------------------------------

static void IndexesScanner_Free(IndexesScanner *scanner) {
  // Let the caller drop its reference (e.g. clear spec->scanner) and free privdata.
  if (scanner->cbs.on_finished) {
    scanner->cbs.on_finished(scanner->privdata, scanner);
  }
  rm_free(scanner->name);
  // Free the last scanned key
  if (scanner->OOMkey) {
    RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
  }
  rm_free(scanner);
}

void IndexScan_Cancel(IndexesScanner *scanner) {
  scanner->cancelled = true;
}

bool IndexScan_IsCancelled(const IndexesScanner *scanner) {
  return scanner->cancelled;
}

size_t IndexScan_ScannedKeys(const IndexesScanner *scanner) {
  return scanner->scannedKeys;
}

static void IndexesScanner_ResetProgression(IndexesScanner *scanner) {
  scanner->scanFailedOnOOM = false;
  scanner->scannedKeys = 0;
}

//---------------------------------------------------------------------------------------------

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

  switch (scanner->cbs.process_key(scanner->privdata, ctx, keyname, key)) {
    case INDEX_SCAN_CONTINUE: ++scanner->scannedKeys; break;
    case INDEX_SCAN_SKIP:     break;
    case INDEX_SCAN_STOP:     scanner->cancelled = true; break;
  }
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
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", scanner->name);
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
                          scanner->name);
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
        RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%zu)",
                        scanner->scannedKeys);
      } else {
        RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled (scanned=%zu)",
                    scanner->name, scanner->scannedKeys);
        goto end;
      }
    }
  }

  if (scanner->isDebug) {
    DebugIndexesScanner* dScanner = (DebugIndexesScanner*)scanner;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_DONE;
  }

  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%zu)",
                    scanner->scannedKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%zu)",
                    scanner->name, scanner->scannedKeys);
  }

end:
  IndexesScanner_Free(scanner);

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

IndexesScanner *IndexScan_Start(const char *name, bool global,
                                const IndexScanCallbacks *cbs, void *privdata) {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "reindex");
  }

  IndexesScanner *scanner;
  // The debug (pause/step) scanner only applies to single-index scans under FT.DEBUG.
  if (!global && globalDebugCtx.debugMode) {
    DebugIndexesScanner *dScanner = rm_calloc(1, sizeof(DebugIndexesScanner));
    dScanner->maxDocsTBscanned = globalDebugCtx.bgIndexing.maxDocsTBscanned;
    dScanner->maxDocsTBscannedPause = globalDebugCtx.bgIndexing.maxDocsTBscannedPause;
    dScanner->wasPaused = false;
    dScanner->status = DEBUG_INDEX_SCANNER_CODE_NEW;
    dScanner->pauseOnOOM = globalDebugCtx.bgIndexing.pauseOnOOM;
    dScanner->pauseBeforeOOMRetry = globalDebugCtx.bgIndexing.pauseBeforeOOMretry;
    scanner = &dScanner->base;
    scanner->isDebug = true;
    // If we need to pause before the scan, set the pause flag
    if (globalDebugCtx.bgIndexing.pauseBeforeScan) {
      globalDebugCtx.bgIndexing.pause = true;
    }
  } else {
    scanner = rm_calloc(1, sizeof(IndexesScanner));
    scanner->isDebug = false;
  }

  scanner->global = global;
  scanner->name = name ? rm_strdup(name) : NULL;
  scanner->cbs = *cbs;
  scanner->privdata = privdata;

  redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner, THPOOL_PRIORITY_HIGH);
  return scanner;
}

void ReindexPool_ThreadPoolDestroy() {
  if (reindexPool != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(reindexPool);
    reindexPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Debug Scanner Functions

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
