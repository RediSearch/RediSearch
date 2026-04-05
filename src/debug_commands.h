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

// ============================================================================
// Unified Debug Pause Points
// ============================================================================
//
// Each pause point is a separate slot in a global array, indexed by enum.
// This keeps the coordinator and shard (which share a process) from
// interfering with each other's pauses, while avoiding code duplication.
//
// Dimensions:
//   Query type:  SEARCH/AGG  vs  HYBRID
//   What:        SHARD results  |  SHARD cursors (HYBRID) |  COORD results

typedef enum {
  PAUSE_POINT_SHARD_STORE_RESULTS,          // shard: AREQ_StoreResults (search/agg)
  PAUSE_POINT_SHARD_HYBRID_STORE_RESULTS,   // shard: HREQ_StoreResults (hybrid)
  PAUSE_POINT_SHARD_HYBRID_STORE_CURSORS,   // shard: hybrid cursor storage
  PAUSE_POINT_COORD_HYBRID_SEND_CHUNK,      // coordinator: sendChunk_hybrid
  PAUSE_POINT_COUNT
} DebugPausePoint;

typedef struct DebugPauseCtx {
  atomic_bool pauseBeforeEnabled;
  atomic_bool pauseAfterEnabled;
  atomic_bool pause;
} DebugPauseCtx;

// Unified DebugPauseCtx API — all take a DebugPausePoint to select the slot
bool DebugPause_IsPauseBeforeEnabled(DebugPausePoint p);
void DebugPause_SetPauseBeforeEnabled(DebugPausePoint p, bool enabled);
bool DebugPause_IsPauseAfterEnabled(DebugPausePoint p);
void DebugPause_SetPauseAfterEnabled(DebugPausePoint p, bool enabled);
bool DebugPause_IsPaused(DebugPausePoint p);
void DebugPause_SetPause(DebugPausePoint p, bool pause);

// Generic debug pause for hybrid paths — shared by shard and coordinator.
// Parameterized by DebugPausePoint so each path has its own isolated slot.
struct HybridRequest;
void debugPauseHybridGeneric(struct HybridRequest *hreq, DebugPausePoint p, bool before);

// ============================================================================
// Named Sync Points for deterministic concurrency testing
// ============================================================================

// Predefined sync point names for query execution
// These correspond to specific locations in the query execution path
#define SYNC_POINT_AFTER_ITERATOR_CREATE  "AfterIteratorCreate"
#define SYNC_POINT_BEFORE_FIRST_READ      "BeforeFirstRead"

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
