/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Background index scan / reindex subsystem. Owns the keyspace scan that populates
// an index on FT.CREATE-over-existing-data: the scanner lifecycle, the per-key
// indexing callback, the background-indexing memory-limit handling, the reindex
// thread pool, and the debug scanner used by FT.DEBUG. Extracted from spec.c.

#ifndef INDEX_SCAN_H__
#define INDEX_SCAN_H__

#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

// Status codes reported by the debug scanner (FT.DEBUG), indexing into
// DEBUG_INDEX_SCANNER_STATUS_STRS.
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

// The single global scanner used by Indexes_ScanAndReindex (scan-all). NULL when no
// global scan is in progress.
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

void IndexesScanner_Cancel(IndexesScanner *scanner);
void IndexesScanner_ResetProgression(IndexesScanner *scanner);
void IndexesScanner_Free(IndexesScanner *scanner);

// Fraction (0..1) of the keyspace already scanned for this spec; 1.0 when no scan is
// in progress. Used by FT.INFO.
double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp);

// Schedule a background scan+reindex of existing keys for a single index. Assumes the
// spec is in a safe state to set a scanner on it (write lock or main thread).
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef spec_ref);

// Schedule a background scan+reindex across all indexes (single global scanner).
void Indexes_ScanAndReindex(void);

// Tear down the reindex thread pool (on shutdown / FT.DEBUG).
void ReindexPool_ThreadPoolDestroy(void);

#ifdef __cplusplus
}
#endif

#endif  // INDEX_SCAN_H__
