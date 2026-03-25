/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_SCANNER_H
#define SPEC_SCANNER_H

#include "redismodule.h"
#include "util/references.h"
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;

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

extern size_t pending_global_indexing_ops;
extern struct IndexesScanner *global_spec_scanner;

void IndexesScanner_Cancel(struct IndexesScanner *scanner);
void IndexesScanner_ResetProgression(struct IndexesScanner *scanner);

void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef ref);
void Indexes_ScanAndReindex(void);
void ReindexPool_ThreadPoolDestroy(void);

// This function is called in case the server starts RDB loading.
void Indexes_StartRDBLoadingEvent(RedisModuleCtx *ctx);

// This function is called in case the server ends RDB loading.
void Indexes_EndRDBLoadingEvent(RedisModuleCtx *ctx);

// This function is to be called when loading finishes (failed or not)
void Indexes_EndLoading(void);

/**
 * Set memory info (used_memory, memoryLimit) from the server.
 * Called by IndexSpec_CreateNew, IndexSpec_AddFields, and VecSimIndex_validate_params.
 */
void setMemoryInfo(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif // SPEC_SCANNER_H
