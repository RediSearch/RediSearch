/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_REQUEST_H__
#define QUERY_REQUEST_H__

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "query_error.h"
#include "util/rs_atomic.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RLookup RLookup;
typedef struct RedisModuleString RedisModuleString;
typedef struct PLN_ArrangeStep PLN_ArrangeStep;
typedef struct ResultProcessor ResultProcessor;
typedef struct SearchResult SearchResult;
struct Cursor;
struct MRChannel;

#define QUERY_OFFSET_NONE UINT32_MAX

/** Cached variables used while serializing stored results. */
typedef struct {
  RLookup *lastLookup;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

/**
 * State retained while results wait for the main-thread reply callback.
 *
 * The cursor owns its request through the request wrapper, not vice versa.
 * The cursor pointer stored here is non-owning and exists only so the reply
 * callback can pause or free it after serialization determines whether the
 * cursor is depleted. It must be cleared after the cursor is handled.
 */
typedef struct {
  SearchResult **results;  // Aggregated results array (NULL if not stored)
  int rc;                  // Pipeline return code (RS_RESULT_OK, RS_RESULT_EOF, etc.)
  bool hasStoredResults;   // Whether results are available to the reply callback
  QueryError err;          // Error copied from the pipeline's temporary QueryError
  cachedVars cv;           // Cached lookup variables used during serialization
  struct Cursor *cursor;   // Non-owning cursor handle for the reply callback
  size_t limit;            // Original limit, used to calculate the RESP2 result length
} ChunkReplyState;

typedef enum {
  QUERY_REQUEST_KIND_AREQ,
  QUERY_REQUEST_KIND_HYBRID,
} QueryRequestKind;

typedef enum {
  CURSOR_DISPOSITION_NONE,   // No cursor is awaiting end-of-cycle handling.
  CURSOR_DISPOSITION_PAUSE,  // Return the cursor to the idle list for another read.
  CURSOR_DISPOSITION_FREE,   // Destroy the cursor when the active cycle ends.
} CursorDisposition;

typedef struct {
  uint64_t id;
  /* Cursor disposition for this cycle. The point that decides the cursor's
   * fate records it here; OnFree executes it, keeping the cursor unreachable
   * to other clients until the cycle has fully ended. The pointer is NULL when
   * the cycle has no cursor; inline execution disposes the cursor directly. */
  struct Cursor *cursor;
  CursorDisposition disposition;
} CursorInfo;

typedef enum {
  REGISTRY_ENTRY_NONE,    // The request has no active registry entry.
  REGISTRY_ENTRY_QUERY,   // The node belongs to the blocked-query registry.
  REGISTRY_ENTRY_CURSOR,  // The node belongs to the blocked-cursor registry.
} RegistryEntryKind;

typedef struct {
  /* TRANSITIONAL(MOD-16691): per-cycle registry bridge, until the request is
   * linked directly into BlockedQueries. The node is unlinked and freed at the
   * end of the cycle; its kind identifies the registry list that owns it. */
  void *node;
  RegistryEntryKind kind;
} RegistryInfo;

typedef struct {
  // Held command arguments borrowed by the request plan. QueryRequest retains
  // their Redis string references until the request is destroyed.
  RedisModuleString **argv;

  // Number of held arguments; may include a trailing debug-only section.
  uint32_t argc;

  // Logical command length available to parsing. Debug requests lower this
  // while keeping the excluded trailing arguments held in argv.
  uint32_t parseArgc;

  // Index of this request's query string in argv. QUERY_OFFSET_NONE denotes a
  // request without a query argument, such as a filterless VSIM subquery.
  uint32_t queryOffset;
} QueryRequestArgs;

typedef struct QueryRequestTimeout {
  RS_Atomic(bool) timedOut;
  bool skipTimeoutChecks;
} QueryRequestTimeout;

static inline bool QueryRequestTimeout_GetTimedOut(const QueryRequestTimeout *timeout) {
  return RS_AtomicBoolLoadRelaxed(&timeout->timedOut);
}

static inline void QueryRequestTimeout_SetTimedOut(QueryRequestTimeout *timeout) {
  RS_AtomicBoolStoreRelaxed(&timeout->timedOut, true);
}

static inline void QueryRequestTimeout_ClearTimedOut(QueryRequestTimeout *timeout) {
  RS_AtomicBoolStoreRelaxed(&timeout->timedOut, false);
}

static inline bool QueryRequestTimeout_ShouldCheck(const QueryRequestTimeout *timeout) {
  return !timeout->skipTimeoutChecks;
}

static inline void QueryRequestTimeout_SetSkipChecks(QueryRequestTimeout *timeout,
                                                     bool skipTimeoutChecks) {
  timeout->skipTimeoutChecks = skipTimeoutChecks;
}

/**
 * Timeout-only synchronization between query workers and main-thread callbacks.
 * TODO($$$): Remove this temporary state after MOD-17486 is merged.
 */
typedef struct QueryRequestAsyncState {
  /* The CAS claim grants exclusive ownership of result production: the BG
   * winner runs the pipeline and stores results, while the timeout-callback
   * winner preempts BG and replies empty. The loser waits for completion.
   * Gated by requiresAggregateResultsSync. MOD-17486 replaces this claim/wait
   * protocol with a timeout callback that never waits on BG state. */
  bool requiresAggregateResultsSync;   // Enable CAS/Signal/Wait around result production
  RS_Atomic(bool) aggregatingResults;  // CAS claim shared by BG and timeout callback
  bool aggregateResultsClaimLost;      // BG lost the claim to the timeout callback
  bool aggregateResultsDone;           // Completion latch guarded by aggregateResultsLock
  /* RP_SAFE_LOADER deadlock-avoidance handshake. A BG worker increments this
   * before taking the GIL and decrements it after release, under
   * aggregateResultsLock. The timeout callback uses it to avoid waiting while
   * holding the GIL. This is a count because hybrid pipelines share the state. */
  int safeLoadersHoldingGIL;
  /* TRANSITIONAL(MOD-16691): per-cycle dequeue latch for coordinator
   * RETURN_STRICT cursor reads. BG and the timeout callback race to claim it;
   * a timeout winner replies without waiting, while a timeout loser may wait
   * because a started read always stores a reply and signals completion.
   * Deleted by the RETURN_STRICT flip. */
  RS_Atomic(int) strictReadOwner;
  RS_Atomic(int) execPhase;
  struct MRChannel *abortWakeChannel;
  pthread_mutex_t abortWakeLock;
  pthread_mutex_t aggregateResultsLock;
  pthread_cond_t aggregateResultsCond;
} QueryRequestAsyncState;

static inline int QueryRequestAsyncState_GetExecutionPhase(const QueryRequestAsyncState *state) {
  return RS_AtomicIntLoadRelaxed(&state->execPhase);
}

static inline void QueryRequestAsyncState_SetExecutionPhase(QueryRequestAsyncState *state,
                                                            int phase) {
  RS_AtomicIntStoreRelaxed(&state->execPhase, phase);
}

void QueryRequestAsyncState_RegisterAbortWakeChannel(QueryRequestAsyncState *state,
                                                     struct MRChannel *channel);
void QueryRequestAsyncState_UnregisterAbortWakeChannel(QueryRequestAsyncState *state);
void QueryRequestAsyncState_WakeAbortChannel(QueryRequestAsyncState *state);

typedef struct QueryRequest {
  QueryRequestKind kind;
  QueryRequestArgs args;
  /* A blocked-client cycle is one initial query execution or cursor read.
   * This is set after RedisModule_BlockClient returns and cleared by OnFree;
   * per-cycle state must not be read while it is false.
   * TODO($$$): Temporary marker intended to replace BlockedRequestCtx.bc. */
  bool blockedClientCycleActive;
  CursorInfo cursorInfo;
  RegistryInfo registryInfo;
  /* Stored results and errors written by BG before UnblockClient and consumed
   * by the main-thread reply or timeout callback. Reset at the end of each
   * cycle and again during request destruction as a safety net. */
  ChunkReplyState reply;
  /* false: BG replies inline through a thread-safe context; true: BG stores
   * results and the Redis reply callback serializes them on the main thread. */
  bool useReplyCallback;
  QueryRequestTimeout timeout;
  QueryRequestAsyncState async;
  /**
   * Transitional reference to the legacy QueryProcessingCtx.endProc slot.
   * The extra indirection makes changes to that slot immediately visible here,
   * without mirroring every pipeline mutation.
   * TODO($$$): Once QueryRequest owns endProc, replace this with ResultProcessor *.
   */
  ResultProcessor **endProcRef;
} QueryRequest;

static inline void QueryRequest_SetEndProcRef(QueryRequest *request,
                                              ResultProcessor **endProcRef) {
  request->endProcRef = endProcRef;
}

static inline ResultProcessor *QueryRequest_GetEndProc(const QueryRequest *request) {
  return request->endProcRef ? *request->endProcRef : NULL;
}

static inline bool QueryRequest_UsesReplyCallback(const QueryRequest *request) {
  return request->useReplyCallback;
}

static inline void QueryRequest_SetUseReplyCallback(QueryRequest *request, bool useReplyCallback) {
  request->useReplyCallback = useReplyCallback;
}

static inline int QueryRequest_GetExecutionPhase(const QueryRequest *request) {
  return QueryRequestAsyncState_GetExecutionPhase(&request->async);
}

// Preserve the phase where a timeout was first observed.
static inline void QueryRequest_SetExecutionPhase(QueryRequest *request, int phase) {
  if (!QueryRequestTimeout_GetTimedOut(&request->timeout)) {
    QueryRequestAsyncState_SetExecutionPhase(&request->async, phase);
  }
}

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind, RedisModuleString **argv,
                       uint32_t argc);
void QueryRequest_HoldArgs(QueryRequest *request, RedisModuleString **argv, uint32_t argc);
void QueryRequest_ResetReply(QueryRequest *request);
void QueryRequest_Destroy(QueryRequest *request);

#ifdef __cplusplus
}
#endif

#endif
