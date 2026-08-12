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
  struct Cursor *cursor;
  CursorDisposition disposition;
} CursorInfo;

typedef enum {
  REGISTRY_ENTRY_NONE,    // The request has no active registry entry.
  REGISTRY_ENTRY_QUERY,   // The node belongs to the blocked-query registry.
  REGISTRY_ENTRY_CURSOR,  // The node belongs to the blocked-cursor registry.
} RegistryEntryKind;

typedef struct {
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

/**
 * Timeout-only synchronization between query workers and main-thread callbacks.
 * TODO($$$): Remove this temporary state after MOD-17486 is merged.
 */
typedef struct QueryRequestAsyncState {
  bool requiresAggregateResultsSync;
  RS_Atomic(bool) aggregatingResults;
  bool aggregateResultsClaimLost;
  bool aggregateResultsDone;
  int safeLoadersHoldingGIL;
  // Per-cycle CAS owner for coordinator RETURN_STRICT cursor reads.
  RS_Atomic(int) strictReadOwner;
  RS_Atomic(int) execPhase;
  struct MRChannel *abortWakeChannel;
  pthread_mutex_t abortWakeLock;
  // TODO($$$): Plug both primitives into the async synchronization paths later in this PR.
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
  // TODO($$$): Temporary marker intended to replace BlockedRequestCtx.bc.
  bool blockedClientCycleActive;
  CursorInfo cursorInfo;
  // TODO($$$): Replace the legacy BRC registry node and type flag with this field.
  RegistryInfo registryInfo;
  ChunkReplyState reply;
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
