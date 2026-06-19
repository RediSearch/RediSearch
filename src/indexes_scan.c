/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// indexes_scan.c -- background scanner / reindex subsystem.
//
// Owns the IndexesScanner family, the debug scanner, the reindex thread pool,
// and the full-rescan / legacy-upgrade entry points. A self-contained subsystem
// with a one-directional dependency on the IndexSpec core (spec.h) and a
// data-only read of the global registry (specDict_g, declared in indexes.h).

#include <assert.h>
#include <unistd.h>

#include "spec.h"
#include "indexes.h"
#include "indexes_scanner.h"
#include "indexes_scan.h"
#include "indexes_asyncscan.h"
#include "search_disk.h"
#include "document.h"
#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/rm_assert.h"
#include "trie/trie.h"
#include "rmalloc.h"
#include "config.h"
#include "redis_index.h"
#include "indexer.h"
#include "rules.h"
#include "doc_types.h"
#include "util/workers.h"
#include "debug_commands.h"
#include "info/info_redis/threads/current_thread.h"
#include "util/redis_mem_info.h"

extern DebugCTX globalDebugCtx;

// Registry globals (data-only dependency): specDict_g/specIdDict_g are defined
// in indexes.c, legacySpecDict in spec.c.
extern dict *specDict_g;
extern dict *specIdDict_g;
extern dict *legacySpecDict;

// Debug scanner functions
static void DebugIndexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             DebugIndexesScanner *dScanner);
static void DebugIndexesScanner_pauseCheck(DebugIndexesScanner* dScanner, RedisModuleCtx *ctx, bool pauseField, DebugIndexScannerCode code);

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

static redisearch_thpool_t *reindexPool = NULL;

//---------------------------------------------------------------------------------------------

static void IndexSpec_DoneIndexingCallabck(struct RSAddDocumentCtx *docCtx, RedisModuleCtx *ctx,
                                           void *pd) {
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
      if (SchemaRule_ShouldIndex(sp, keyname, type, NULL)) {
        IndexSpec_UpdateDoc(sp, ctx, keyname, type, NULL);
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
        RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%zu)",
                        scanner->scannedKeys);
      } else {
        RedisModule_Log(ctx, "notice", "Scanning index %s in background: cancelled (scanned=%zu)",
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
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%zu)",
                    scanner->scannedKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%zu)",
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

  // Route disk indexes to the AsyncScan driver; RAM keeps the synchronous scan
  // (in-RAM key loads are cheap and gain nothing from offloading the read). The
  // disk AsyncScan API is guaranteed present when SearchDisk_IsEnabled().
  redisearch_thpool_proc reindexTask = SearchDisk_IsEnabled()
      ? (redisearch_thpool_proc)Indexes_AsyncScanAndReindexTask
      : (redisearch_thpool_proc)Indexes_ScanAndReindexTask;

  redisearch_thpool_add_work(reindexPool, reindexTask, scanner, THPOOL_PRIORITY_HIGH);
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

// only used on "RDB load finished" event (before the server is ready to accept commands)
// so it threadsafe
void IndexSpec_DropLegacyIndexFromKeySpace(IndexSpec *sp) {
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(RSDummyContext, sp);

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  size_t termLen;

  TrieIterator *it = Trie_IterateAll(ctx.spec->terms);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, NULL)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModuleString *keyName = Legacy_fmtRedisTermKey(&ctx, res, strlen(res));
    Redis_LegacyDropScanHandler(ctx.redisCtx, keyName, &ctx);
    RedisModule_FreeString(ctx.redisCtx, keyName);
    rm_free(res);
  }
  TrieIterator_Free(it);

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (size_t i = 0; i < ctx.spec->numFields; i++) {
    const FieldSpec *fs = ctx.spec->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_NUMERIC);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_TAG);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
    if (FIELD_IS(fs, INDEXFLD_T_GEO)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_GEO);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
  }
  HiddenString_LegacyDropFromKeySpace(ctx.redisCtx, INDEX_SPEC_KEY_FMT, sp->specName);
}

void Indexes_UpgradeLegacyIndexes() {
  dictIterator *iter = dictGetIterator(legacySpecDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    IndexSpec_DropLegacyIndexFromKeySpace(sp);

    // recreate the doctable
    DocTable_Free(&sp->docs);
    sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

    // clear index stats
    memset(&sp->stats, 0, sizeof(sp->stats));
    // Init the index error
    sp->stats.indexError = IndexError_Init();

    // put the new index in the global spec dictionaries (by name and by specId)
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);
    dictAdd(specIdDict_g, (void*)(uintptr_t)sp->specId, spec_ref.rm);
  }
  dictReleaseIterator(iter);
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
