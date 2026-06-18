/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEXES_SCAN_H__
#define __INDEXES_SCAN_H__

#include <stdbool.h>
#include <stddef.h>

#include "redismodule.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration: the IndexSpec lifecycle lives in spec.{c,h}. The scanner
// only needs IndexSpec by pointer, so a forward declaration keeps the
// dependency one-directional (indexes_scan.c -> spec.h, never the reverse).
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

void IndexesScanner_Cancel(struct IndexesScanner *scanner);
void IndexesScanner_ResetProgression(struct IndexesScanner *scanner);
void IndexesScanner_Free(IndexesScanner *scanner);

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp);

// Schedule a background scan + reindex of the keyspace into the given spec.
// Assumes that the spec is in a safe state to set a scanner on it (write lock
// or main thread).
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef ref);

// Schedule a background scan + reindex of all registered indexes.
void Indexes_ScanAndReindex();

// Upgrade legacy (pre-RDB-event) indexes by dropping their old keyspace
// representation and publishing them into the global registry.
void Indexes_UpgradeLegacyIndexes();

// Expose reindexpool for debug
void ReindexPool_ThreadPoolDestroy();

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_SCAN_H__
