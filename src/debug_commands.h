/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_DEBUG_COMMADS_H_
#define SRC_DEBUG_COMMADS_H_

#include "redismodule.h"
#include <stdbool.h>
#include <stdatomic.h>

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

// General debug context
typedef struct DebugCTX {
    bool debugMode; // Indicates whether debug mode is enabled
    BgIndexingDebugCtx bgIndexing; // Background indexing debug context
} DebugCTX;

// Should be called after each debug command that changes the debugCtx
void validateDebugMode(DebugCTX *debugCtx);
// Yield counter functions
void IncrementYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);

#endif /* SRC_DEBUG_COMMADS_H_ */
