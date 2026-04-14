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
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
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

// ============================================================================
// Named Sync Points for deterministic concurrency testing
// ============================================================================

// Predefined sync point names for query execution
// These correspond to specific locations in the query execution path
#define SYNC_POINT_AFTER_ITERATOR_CREATE       "AfterIteratorCreate"
#define SYNC_POINT_BEFORE_FIRST_READ           "BeforeFirstRead"
#define SYNC_POINT_BEFORE_DIST_HYBRID_PROMOTE  "BeforeDistHybridPromote"

// SyncPoint API function declarations
// Arm a sync point - subsequent calls to SyncPoint_Wait will block
// Returns true on success, false if max sync points reached
// NOTE: Not thread-safe. Must only be called from the main thread.
bool SyncPoint_Arm(const char *name);
// Signal a waiting thread at the named sync point to continue (also disarms it)
void SyncPoint_Signal(const char *name);
// Check if a thread is waiting at the named sync point
bool SyncPoint_IsWaiting(const char *name);
// Check if a sync point is armed
bool SyncPoint_IsArmed(const char *name);
// Clear all sync points
void SyncPoint_ClearAll(void);
// Called from code paths to potentially wait at a sync point
// If the named point is armed, blocks until signaled
void SyncPoint_Wait(const char *name);

#endif  // ENABLE_ASSERT

// Yield counter functions
void IncrementLoadYieldCounter(void);
void IncrementBgIndexYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);
