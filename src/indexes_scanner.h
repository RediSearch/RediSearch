/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEXES_SCANNER_H__
#define __INDEXES_SCANNER_H__

// indexes_scanner.{c,h} -- the IndexesScanner core shared by the two reindex
// strategies. It owns the scanner type, its lifecycle (create/free/cancel), the
// debug-scanner variant, and the background-indexing memory-limit helpers.
//
// Both strategies depend on this core and never on each other:
//
//        indexes_scanner   (this file: scanner state + lifecycle + OOM helpers)
//          ^          ^
//          |          |
//   indexes_scan   indexes_asyncscan   (sync RM_Scan / async RedisModule_AsyncScan)
//
// Keeping the shared state here is what lets the synchronous and AsyncScan drivers
// be siblings: indexes_asyncscan.c includes this header, not indexes_scan.h, so
// there is no dependency cycle between the two strategy translation units.

#include <stdbool.h>
#include <stddef.h>

#include "redismodule.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration: the IndexSpec lifecycle lives in spec.{c,h}. The scanner
// only needs IndexSpec by pointer, so a forward declaration keeps the
// dependency one-directional (indexes_scanner.c -> spec.h, never the reverse).
typedef struct IndexSpec IndexSpec;

///////////////////////////////////////////////////////////////////////////////////////////////

typedef enum {
    DEBUG_INDEX_SCANNER_CODE_NEW,
    DEBUG_INDEX_SCANNER_CODE_RUNNING,
    DEBUG_INDEX_SCANNER_CODE_DONE,
    DEBUG_INDEX_SCANNER_CODE_CANCELLED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED,
    DEBUG_INDEX_SCANNER_CODE_RESUMED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_ON_OOM,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_BEFORE_OOM_RETRY,

    //Insert new codes here (before COUNT)
    DEBUG_INDEX_SCANNER_CODE_COUNT  // Helps with array size checks
    //Do not add new codes after COUNT
} DebugIndexScannerCode;

extern const char *DEBUG_INDEX_SCANNER_STATUS_STRS[];

extern size_t pending_global_indexing_ops;
extern struct IndexesScanner *global_spec_scanner;

typedef struct IndexesScanner {
  bool global;
  bool cancelled;
  bool isDebug;
  bool scanFailedOnOOM;
  WeakRef spec_ref;
  char *spec_name_for_logs;
  size_t scannedKeys;
  RedisModuleString *OOMkey; // The key that caused the OOM
} IndexesScanner;

typedef struct DebugIndexesScanner {
  IndexesScanner base;
  int maxDocsTBscanned;
  int maxDocsTBscannedPause;
  bool wasPaused;
  bool pauseOnOOM;
  int status;
  bool pauseBeforeOOMRetry;
} DebugIndexesScanner;

// Scanner lifecycle. A scanner is created by the dispatcher, handed to a strategy
// task (sync or async), and freed by that task when the scan ends.
IndexesScanner *IndexesScanner_NewGlobal();
IndexesScanner *IndexesScanner_New(StrongRef global_ref);
DebugIndexesScanner *DebugIndexesScanner_New(StrongRef global_ref);
void IndexesScanner_Free(IndexesScanner *scanner);
void IndexesScanner_Cancel(struct IndexesScanner *scanner);
void IndexesScanner_ResetProgression(struct IndexesScanner *scanner);

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp);

// Record a background-indexing OOM failure on the scanner's spec so it is visible
// to clients: sets the spec's scan_failed_OOM flag (consulted at query time to warn
// that results may be incomplete, and aggregated in FT.INFO), records `error` as the
// spec's last indexing error, and raises the background-index failure flag (the
// FT.INFO "background indexing status" field). `error` must carry no user data. The
// offending key is taken from scanner->OOMkey, which may be NULL when no single key
// is to blame (the async engine-OOM case). No-op for the global scanner or a spec
// that has since been dropped. Does not touch scanner->cancelled — callers own their
// control flow. Callers MUST hold the GIL: the spec's IndexError is read by FT.INFO on
// the main thread, and — crucially — IndexError_AddError mutates the refcount of the
// shared, non-atomic NA_rstr sentinel, which a per-spec lock cannot serialize (see the
// definition for the full rationale). Shared by both reindex strategies.
void IndexesScanner_RecordBackgroundOOMFailure(RedisModuleCtx *ctx, IndexesScanner *scanner,
                                               const char *error);

// Cancel the scan and record an OOM failure on the spec (FT.INFO error + log).
// Used by the synchronous strategy's module-side memory check.
void scanStopAfterOOM(RedisModuleCtx *ctx, IndexesScanner *scanner);

// Return true if used_memory exceeds (indexingMemoryLimit % × memoryLimit);
// false if within bounds or the limit is 0. Shared by both reindex strategies.
bool isBgIndexingMemoryOverLimit(RedisModuleCtx *ctx);

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_SCANNER_H__
