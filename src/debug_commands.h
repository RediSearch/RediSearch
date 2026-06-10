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
int parseDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status, unsigned long long *debug_params_count);

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

// Sentinel for the pauseAfterN field of AggregateResultsDebugCtx
#define AGGREGATE_RESULTS_NO_PAUSE 0  // Disable pause (no pause point set)

// Struct used for debugging the AggregateResults loop in aggregate_exec_common.c
// (pause after extracting N results from the pipeline). Only available in debug
// builds to avoid affecting release performance.
typedef struct AggregateResultsDebugCtx {
  atomic_bool pause;          // Atomic bool to wait for the resume command
  atomic_int  pauseAfterN;    // AGGREGATE_RESULTS_NO_PAUSE, or N>0 to pause after the Nth result is extracted
  atomic_int  resultsCount;   // Counter of results extracted so far
} AggregateResultsDebugCtx;

// AggregateResultsDebugCtx API function declarations
bool AggregateResultsDebugCtx_IsPaused(void);
void AggregateResultsDebugCtx_SetPause(bool pause);
int  AggregateResultsDebugCtx_GetPauseAfterN(void);
void AggregateResultsDebugCtx_SetPauseAfterN(int n);
void AggregateResultsDebugCtx_IncrementResultsCount(void);
int  AggregateResultsDebugCtx_GetResultsCount(void);

// Scope selector for StoreResults pauses: restricts which AREQ/HybridRequest
// populations the global pause applies to (matches the INTERNAL_ONLY token
// convention used in src/aggregate/aggregate_debug.c).
typedef enum {
  STORE_RESULTS_SCOPE_BOTH = 0,         // Default: pause for both internal and non-internal requests
  STORE_RESULTS_SCOPE_INTERNAL_ONLY,    // Pause only for internal (coordinator-dispatched) requests
  STORE_RESULTS_SCOPE_NON_INTERNAL_ONLY,// Pause only for non-internal (user-facing) requests
} StoreResultsScope;

// Struct used for debugging store results (pause before/after AREQ_StoreResults and HREQ_StoreResults)
// Only available in debug builds to avoid affecting release performance
typedef struct StoreResultsDebugCtx {
  atomic_bool pauseBeforeEnabled;   // Whether pause before StoreResults is enabled
  atomic_bool pauseAfterEnabled;    // Whether pause after StoreResults is enabled
  atomic_int  scope;                // StoreResultsScope; updated together with the enable flags
  atomic_bool pause;                // Atomic bool to wait for the resume command
} StoreResultsDebugCtx;

// StoreResultsDebugCtx API function declarations
bool StoreResultsDebugCtx_IsPauseBeforeEnabled(void);
void StoreResultsDebugCtx_SetPauseBeforeEnabled(bool enabled, StoreResultsScope scope);
bool StoreResultsDebugCtx_IsPauseAfterEnabled(void);
void StoreResultsDebugCtx_SetPauseAfterEnabled(bool enabled, StoreResultsScope scope);
StoreResultsScope StoreResultsDebugCtx_GetScope(void);
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
#define SYNC_POINT_BEFORE_CURSOR_READ_SPEC_PROMOTE      "BeforeCursorReadSpecPromote"
#define SYNC_POINT_BEFORE_AGGREGATE_RESULTS_CLAIM       "BeforeAggregateResultsClaim"
#define SYNC_POINT_BEFORE_RPNET_START                   "BeforeRPNetStart"
#define SYNC_POINT_BEFORE_RPNET_NEXT                    "BeforeRPNetNext"
#define SYNC_POINT_AFTER_ITERATOR_START                 "AfterIteratorStart"
#define SYNC_POINT_RPNET_REPLY_ADMITTED                 "RpnetReplyAdmitted"
#define SYNC_POINT_RPNET_WAITING_FOR_REPLY              "RpnetWaitingForReply"
#define SYNC_POINT_BEFORE_QI_TIMEOUT_CHECK              "BeforeQITimeoutCheck"
// Stalls a shard's internal (_FT.HYBRID) background execution before it produces
// its cursor-mapping reply, so the coordinator's ProcessHybridCursorMappings sees
// an alive-but-unresponsive shard (no reply, no disconnect). Reproduces MOD-16145.
#define SYNC_POINT_BEFORE_HYBRID_SHARD_REPLY            "BeforeHybridShardReply"

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

// Shard dispatch fault injection (test-only, ENABLE_ASSERT builds): arm the next
// `count` MRCluster_SendCommand calls to return REDIS_ERR, so DebugSendError_Consume
// returns true that many times. Exercises the no-reply error path.
void DebugSendError_Arm(int count);
// Consume one armed failure; returns true if the caller should treat the send as
// failed. Thread-safe.
bool DebugSendError_Consume(void);

// Process-wide counter of threads parked in `RedisSearchCtx_LockSpecWrite`
// waiting on a spec rwlock. Bumped before `pthread_rwlock_wrlock` and
// decremented once the write lock has been acquired. Used by tests (sync-point
// stop predicates) to observe a pending writer without depending on the main
// thread, since the main thread is exactly what's blocked on the wrlock in the
// scenarios these tests cover.
void PendingSpecWriters_Incr(void);
void PendingSpecWriters_Decr(void);
uint32_t PendingSpecWriters_Get(void);

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

// Coord request ctx free counter. Bumped on every CoordRequestCtx_Free so
// tests can deterministically observe that free_privdata has fired without
// blocking the main thread inside the callback.
void CoordReqCtxFreeDebug_Increment(void);
uint64_t CoordReqCtxFreeDebug_GetCount(void);

// Tracks the currently active coordinator MRIterator so tests can poll the
// `pending` shard counter via FT.DEBUG BG_PENDING_REPLIES. Set after the
// iterator is created in the RPNet start path; cleared before it is released
// in rpnetFree. Only one query is expected to be active at a time in tests.
struct MRIterator;
void DebugBgIterator_Set(struct MRIterator *it);
void DebugBgIterator_Clear(struct MRIterator *it);

#endif  // ENABLE_ASSERT


// Yield counter functions
void IncrementLoadYieldCounter(void);
void IncrementBgIndexYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);
