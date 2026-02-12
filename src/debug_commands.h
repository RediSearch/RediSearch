/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include  <stdbool.h>
#include <stdatomic.h>
#include "result_processor.h"

#define RS_DEBUG_FLAGS 0, 0, 0
#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

int RegisterDebugCommands(RedisModuleCommand *debugCommand);

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

#ifdef ENABLE_ASSERT
// Struct used for debugging coordinator reduction (pause mid-reduce)
// Only available in debug builds to avoid affecting release performance
typedef struct CoordReduceDebugCtx {
  atomic_bool pause;           // Atomic bool to wait for the resume command
  atomic_int pauseBeforeN;     // N value: 0=no pause, -1=pause after last, N>0=pause before Nth result
  atomic_int reduceCount;      // Counter of results reduced so far
} CoordReduceDebugCtx;

// CoordReduceDebugCtx API function declarations
bool CoordReduceDebugCtx_IsPaused(void);
void CoordReduceDebugCtx_SetPause(bool pause);
int CoordReduceDebugCtx_GetPauseBeforeN(void);
void CoordReduceDebugCtx_SetPauseBeforeN(int n);
void CoordReduceDebugCtx_IncrementReduceCount(void);
int CoordReduceDebugCtx_GetReduceCount(void);

// Struct used for debugging reply (pause before reply before acquiring lock)
// Only available in debug builds to avoid affecting release performance
typedef struct ReplyDebugCtx {
  atomic_bool pause;           // Atomic bool to wait for the resume command
  atomic_bool shouldPause;     // Whether to pause before reply
} ReplyDebugCtx;

// ReplyDebugCtx API function declarations
bool ReplyDebugCtx_IsPaused(void);
void ReplyDebugCtx_SetPause(bool pause);
bool ReplyDebugCtx_ShouldPause(void);
void ReplyDebugCtx_SetShouldPause(bool shouldPause);
void ReplyDebugCtx_CheckAndPause(void);
#endif

// Yield counter functions
void IncrementLoadYieldCounter(void);
void IncrementBgIndexYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);
