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
// Named sentinel values for the pauseBeforeN field of CoordReduceDebugCtx
#define COORD_REDUCE_NO_PAUSE                0   // Disable pause (no pause point set)
#define COORD_REDUCE_PAUSE_AFTER_LAST_RESULT (-1) // Pause after the last result is reduced
#define COORD_REDUCE_PAUSE_BEFORE_REDUCER_INIT (-2) // Pause after claiming reducing but before reducer context init

// Struct used for debugging coordinator reduction (pause mid-reduce)
// Only available in debug builds to avoid affecting release performance
typedef struct CoordReduceDebugCtx {
  atomic_bool pause;           // Atomic bool to wait for the resume command
  atomic_int pauseBeforeN;     // COORD_REDUCE_NO_PAUSE, COORD_REDUCE_PAUSE_BEFORE_REDUCER_INIT,
                               // COORD_REDUCE_PAUSE_AFTER_LAST_RESULT, or N>0 to pause before the Nth result
  atomic_int reduceCount;      // Counter of results reduced so far
} CoordReduceDebugCtx;

// CoordReduceDebugCtx API function declarations
bool CoordReduceDebugCtx_IsPaused(void);
void CoordReduceDebugCtx_SetPause(bool pause);
int CoordReduceDebugCtx_GetPauseBeforeN(void);
void CoordReduceDebugCtx_SetPauseBeforeN(int n);
void CoordReduceDebugCtx_IncrementReduceCount(void);
int CoordReduceDebugCtx_GetReduceCount(void);

// Struct used for debugging store results (pause before/after AREQ_StoreResults and HREQ_StoreResults)
// Only available in debug builds to avoid affecting release performance
typedef struct StoreResultsDebugCtx {
  atomic_bool pauseBeforeEnabled;   // Whether pause before StoreResults is enabled
  atomic_bool pauseAfterEnabled;    // Whether pause after StoreResults is enabled
  atomic_bool pause;                // Atomic bool to wait for the resume command
} StoreResultsDebugCtx;

// StoreResultsDebugCtx API function declarations
bool StoreResultsDebugCtx_IsPauseBeforeEnabled(void);
void StoreResultsDebugCtx_SetPauseBeforeEnabled(bool enabled);
bool StoreResultsDebugCtx_IsPauseAfterEnabled(void);
void StoreResultsDebugCtx_SetPauseAfterEnabled(bool enabled);
bool StoreResultsDebugCtx_IsPaused(void);
void StoreResultsDebugCtx_SetPause(bool pause);

// ============================================================================
// Named Sync Points for deterministic concurrency testing
// ============================================================================

// Predefined sync point names for query execution
// These correspond to specific locations in the query execution path
#define SYNC_POINT_AFTER_ITERATOR_CREATE                "AfterIteratorCreate"
#define SYNC_POINT_BEFORE_FIRST_READ                    "BeforeFirstRead"
#define SYNC_POINT_BEFORE_DIST_HYBRID_PROMOTE           "BeforeDistHybridPromote"
#define SYNC_POINT_BEFORE_SPEC_LOCK                     "BeforeSpecLock"
#define SYNC_POINT_BEFORE_CURSOR_READ_SEND_CHUNK        "BeforeCursorReadSendChunk"
#define SYNC_POINT_BEFORE_AGGREGATE_RESULTS_CLAIM       "BeforeAggregateResultsClaim"
#define SYNC_POINT_BEFORE_RPNET_START                   "BeforeRPNetStart"
#define SYNC_POINT_AFTER_ITERATOR_START                 "AfterIteratorStart"
#define SYNC_POINT_RPNET_REPLY_ADMITTED                 "RpnetReplyAdmitted"

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

// Predicate callback type for SyncPoint_WaitUntil
typedef bool (*SyncPointStopFn)(void *arg);
// Like SyncPoint_Wait, but also exits the wait loop when `stop_fn(arg)` returns
// true. Lets workers release early when a timeout fires on the main thread.
void SyncPoint_WaitUntil(const char *name, SyncPointStopFn stop_fn, void *arg);

// Struct used for debugging hybrid cursor storage ONLY (pause before/after cursor creation)
// Separate from StoreResultsDebugCtx to allow independent control
typedef struct HybridStoreCursorsDebugCtx {
  atomic_bool pauseBeforeEnabled;   // Whether pause before cursor storage is enabled
  atomic_bool pauseAfterEnabled;    // Whether pause after cursor storage is enabled
  atomic_bool pause;                // Atomic bool to wait for the resume command
} HybridStoreCursorsDebugCtx;

// HybridStoreCursorsDebugCtx API function declarations
bool HybridStoreCursorsDebugCtx_IsPauseBeforeEnabled(void);
void HybridStoreCursorsDebugCtx_SetPauseBeforeEnabled(bool enabled);
bool HybridStoreCursorsDebugCtx_IsPauseAfterEnabled(void);
void HybridStoreCursorsDebugCtx_SetPauseAfterEnabled(bool enabled);
bool HybridStoreCursorsDebugCtx_IsPaused(void);
void HybridStoreCursorsDebugCtx_SetPause(bool pause);

#endif  // ENABLE_ASSERT


// Yield counter functions
void IncrementLoadYieldCounter(void);
void IncrementBgIndexYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);
