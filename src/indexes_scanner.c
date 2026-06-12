/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// indexes_scanner.c -- IndexesScanner core shared by the synchronous (indexes_scan.c)
// and AsyncScan (indexes_asyncscan.c) reindex strategies: scanner state, lifecycle,
// the debug-scanner variant, and the background-indexing memory-limit helpers.

#include <assert.h>

#include "indexes_scanner.h"

#include "spec.h"
#include "config.h"
#include "rmalloc.h"
#include "debug_commands.h"
#include "util/redis_mem_info.h"

extern DebugCTX globalDebugCtx;

// Owned scanner globals.
IndexesScanner *global_spec_scanner = NULL;
size_t pending_global_indexing_ops = 0;

const char *DEBUG_INDEX_SCANNER_STATUS_STRS[] = {
    "NEW", "SCANNING", "DONE", "CANCELLED", "PAUSED", "RESUMED", "PAUSED_ON_OOM", "PAUSED_BEFORE_OOM_RETRY",
};

// Static assertion to ensure array size matches the number of statuses
static_assert(
    (sizeof(DEBUG_INDEX_SCANNER_STATUS_STRS) / sizeof(char*)) == DEBUG_INDEX_SCANNER_CODE_COUNT,
    "Mismatch between DebugIndexScannerCode enum and DEBUG_INDEX_SCANNER_STATUS_STRS array"
);

// This function should be called after the second background scan OOM error
// It will stop the background scan process
void scanStopAfterOOM(RedisModuleCtx *ctx, IndexesScanner *scanner) {
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

// Return true if used_memory exceeds (indexingMemoryLimit % × memoryLimit); false if within bounds or limit is 0.
bool isBgIndexingMemoryOverLimit(RedisModuleCtx *ctx) {
  // if memory limit is set to 0, we don't need to check for memory usage
  if(RSGlobalConfig.indexingMemoryLimit == 0) {
    return false;
  }

  float used_memory_ratio = RedisMemory_GetUsedMemoryRatioUnified(ctx);
  float memory_limit_ratio = (float)RSGlobalConfig.indexingMemoryLimit / 100;

  return (used_memory_ratio > memory_limit_ratio) ;
}

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp) {
  if (scanner || sp->scan_in_progress) {
    if (scanner) {
      size_t totalKeys = RedisModule_DbSize(ctx);
      return totalKeys > 0 ? (double)scanner->scannedKeys / totalKeys : 0;
    } else {
      return 0;
    }
  } else {
    return 1.0;
  }
}

IndexesScanner *IndexesScanner_NewGlobal() {
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

IndexesScanner *IndexesScanner_New(StrongRef global_ref) {

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

DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref) {

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
