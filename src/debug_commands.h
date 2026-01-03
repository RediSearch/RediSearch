/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"
#include <stdbool.h>
#include <stdatomic.h>
#include "result_processor.h"

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Struct used for debugging background indexing
typedef struct BgIndexingDebugCtx {
    int maxDocsTBscanned; // Max number of documents to be scanned before stopping
    int maxDocsTBscannedPause; // Number of documents to be scanned before pausing
    bool pauseBeforeScan; // Whether to pause before scanning
    volatile atomic_bool pause; // Volatile atomic bool to wait for the resume command
    bool pauseOnOOM; // Whether to pause on OOM
    bool pauseBeforeOOMretry; // Whether to pause before the first OOM retry
} BgIndexingDebugCtx;

// Struct used for debugging queries
// Note: unrelated to timeout debugging
typedef struct QueryDebugCtx {
  volatile atomic_bool pause; // Volatile atomic bool to wait for the resume command
  ResultProcessor *debugRP; // Result processor for debugging, supports debugging one query at a time
} QueryDebugCtx;

// General debug context
typedef struct DebugCTX {
  bool debugMode; // Indicates whether debug mode is enabled
  BgIndexingDebugCtx bgIndexing; // Background indexing debug context
  QueryDebugCtx query; // Query debug context
} DebugCTX;

// Should be called after each debug command that changes the debugCtx
// Exception for QueryDebugCtx
void validateDebugMode(DebugCTX *debugCtx);

// QueryDebugCtx API function declarations
bool QueryDebugCtx_IsPaused(void);
void QueryDebugCtx_SetPause(bool pause);
ResultProcessor* QueryDebugCtx_GetDebugRP(void);
void QueryDebugCtx_SetDebugRP(ResultProcessor* debugRP);
bool QueryDebugCtx_HasDebugRP(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);

#ifdef RS_COORDINATOR
// Function pointer for coordinator thread pool control (set by coordinator at init)
// op can be "pause", "resume", etc. - similar to WorkerThreadsSwitch pattern
extern int (*CoordThreadPool_DebugFunc)(const char *op);
#endif
