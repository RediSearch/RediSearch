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
#include <time.h>

#include "config.h"
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
 */
typedef struct {
  SearchResult **results;  // Aggregated results array (NULL if not stored)
  int rc;                  // Pipeline return code (RS_RESULT_OK, RS_RESULT_EOF, etc.)
  bool hasStoredResults;   // Whether results are available to the reply callback
  /* The cycle's error and warnings — the request's single error slot. Hybrid
   * sub-pipelines report into it directly (a sub's results are published by
   * the parent's reply). TRANSITIONAL(MOD-17486): parents still clone the
   * pipeline's stack-local QueryError into it at publication; the
   * RETURN_STRICT flip wires every pipeline directly. */
  QueryError err;
  cachedVars cv;           // Cached lookup variables used during serialization
  size_t limit;            // Original limit, used to calculate the RESP2 result length
} ChunkReplyState;

/** Synchronizes access to reply state when results cross threads. */
typedef struct {
  ChunkReplyState state;
  pthread_mutex_t lock;
} SafeChunkReplyState;

typedef enum {
  QUERY_REQUEST_KIND_AREQ,
  QUERY_REQUEST_KIND_HYBRID,
} QueryRequestKind;

typedef enum {
  /* Destroy the cursor when the active cycle ends. The per-cycle default: a
   * cycle whose reply exposed no live cursor id must free, never park. */
  CURSOR_DISPOSITION_FREE,
  CURSOR_DISPOSITION_PAUSE,  // Return the cursor to the idle list for another read.
} CursorDisposition;

typedef struct {
  uint64_t id;
  /* The cycle's cursor, published when it becomes known (main thread at read
   * dispatch; at reservation for the initial cycle); NULL when the cycle has
   * no cursor. The path that replies a live cursor id records PAUSE;
   * EndCycle executes the disposition, keeping the cursor unreachable to
   * other clients until the cycle has fully ended. Inline execution disposes
   * the cursor directly instead. */
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

#define QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT 100

#ifdef __cplusplus
static_assert(QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT <= UINT8_MAX,
              "Query request timeout counter limit exceeds uint8_t");
#else
_Static_assert(QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT <= UINT8_MAX,
               "Query request timeout counter limit exceeds uint8_t");
#endif

/** Per-execution-cycle timeout source. */
typedef enum {
  QUERY_REQUEST_TIMEOUT_UNARMED,         // No timeout checks are active.
  QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT,  // Main-thread callback publishes an atomic marker.
  QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE,  // Execution thread checks a monotonic deadline.
} QueryRequestTimeoutKind;

/**
 * Request-owned timeout state retained for the request's entire lifetime.
 *
 * Init selects no active source. Each execution cycle selects exactly one
 * source with BeginCycle, and Reset returns the state to UNARMED between
 * cycles. Cursor reads reuse this object, so policy and timeoutMS remain sticky
 * while the source union is reinitialized for each read.
 *
 * Threading contract:
 *
 * - Init, UpdateConfig, Reset, BeginCycle, and GetClockDeadlineForUpdate require
 *   exclusive access. They must run before the request is published or between
 *   execution cycles, after all consumers of the previous cycle have stopped.
 * - kind is immutable during a cycle. Only the union member selected by kind
 *   may be accessed, and source-specific pointers must not outlive that cycle.
 * - During a BLOCKED_CLIENT cycle, the main-thread timeout callback may publish
 *   the marker concurrently with worker reads. The marker is atomic but does
 *   not publish any other request state.
 * - During a CLOCK_DEADLINE cycle, the deadline is immutable. The counter is
 *   shared across amortized call sites but is not atomic, so calls to
 *   IsTimedOut for one request must be serialized.
 *
 * The owning QueryRequest must outlive every direct pointer, source-specific
 * pointer, and foreign-language handle borrowed from this object.
 */
typedef struct QueryRequestTimeout {
  // Stable configuration retained across cursor execution cycles.
  long long timeoutMS;
  RSTimeoutPolicy policy;

  // The active source is changed only between execution cycles, while no
  // consumer can observe the union.
  QueryRequestTimeoutKind kind;
  union {
    RS_Atomic(bool) blockedClientTimedOut;
    struct {
      struct timespec deadline;
      // Shared by all amortized timeout callers during the clock cycle.
      uint8_t counter;
    } clock;
  } source;
} QueryRequestTimeout;

/** Initializes timeout as UNARMED with the supplied sticky configuration. */
void QueryRequestTimeout_Init(QueryRequestTimeout *timeout, RSTimeoutPolicy policy,
                              long long timeoutMS);
/**
 * Updates sticky configuration without changing or rearming the active cycle.
 * The new timeoutMS is used when a later clock cycle begins.
 */
void QueryRequestTimeout_UpdateConfig(QueryRequestTimeout *timeout, RSTimeoutPolicy policy,
                                      long long timeoutMS);
/**
 * Ends the current cycle by selecting UNARMED; sticky configuration and stale
 * union storage are retained.
 */
void QueryRequestTimeout_Reset(QueryRequestTimeout *timeout);
/**
 * Starts a cycle using kind as its timeout source.
 *
 * BLOCKED_CLIENT clears the atomic marker. CLOCK_DEADLINE derives a new
 * deadline from the sticky timeoutMS and resets the shared counter; a zero
 * timeoutMS leaves the state UNARMED. RETURN_STRICT must be downgraded by the
 * consumer before selecting CLOCK_DEADLINE because it requires the
 * blocked-client callback.
 */
void QueryRequestTimeout_BeginCycle(QueryRequestTimeout *timeout, QueryRequestTimeoutKind kind);
/**
 * Atomically publishes a timeout to readers of the active BLOCKED_CLIENT
 * cycle. This is the only state transition permitted concurrently with timeout
 * checks; it communicates only the marker value, not other memory.
 */
void QueryRequestTimeout_MarkTimedOut(QueryRequestTimeout *timeout);

/**
 * Borrows the active BLOCKED_CLIENT flag for a wait that cannot use the
 * source-neutral API. Access must use RS_Atomic operations. The pointer is
 * valid only until the current cycle ends and must not be retained across
 * Reset or BeginCycle.
 */
RS_Atomic(bool) *QueryRequestTimeout_GetBlockedClientFlag(QueryRequestTimeout *timeout);

/**
 * Borrows the active CLOCK_DEADLINE for consumers that have not yet migrated
 * to the source-neutral timeout API. The deadline is immutable and the pointer
 * is valid only until the current cycle ends.
 */
const struct timespec *QueryRequestTimeout_GetClockDeadline(
    const QueryRequestTimeout *timeout);

/**
 * Selects CLOCK_DEADLINE and returns its storage for debug timeout simulation
 * and tests. This exclusive-access operation invalidates pointers to the
 * previous source; the caller must initialize the returned deadline before
 * publishing the cycle to readers.
 */
struct timespec *QueryRequestTimeout_GetClockDeadlineForUpdate(QueryRequestTimeout *timeout);

/**
 * Reports whether the request has timed out, independently of how the timeout
 * is detected, without amortizing clock checks:
 *
 * - UNARMED always returns false.
 * - BLOCKED_CLIENT reads the flag published by the blocked-client callback.
 * - CLOCK_DEADLINE compares the monotonic clock with the active deadline.
 *
 * Use this exact operation only where the caller must observe the deadline
 * immediately, such as after blocking or at an execution boundary. It neither
 * applies the timeout policy nor changes the timeout state. The active source
 * must remain fixed for the duration of the call; BLOCKED_CLIENT may be marked
 * concurrently as described by QueryRequestTimeout.
 */
bool QueryRequestTimeout_IsTimedOutExact(const QueryRequestTimeout *timeout);

/**
 * Reports a timeout only when the active source is BLOCKED_CLIENT.
 *
 * This compatibility operation preserves call sites that historically checked
 * only the blocked-client atomic flag. It deliberately returns false for
 * CLOCK_DEADLINE so migrating those call sites does not introduce new clock
 * checks that could change product behavior.
 * Avoid using this call unless strictly needed. It has the same cycle and
 * concurrency requirements as QueryRequestTimeout_IsTimedOutExact.
 */
bool QueryRequestTimeout_IsBlockedClientTimedOut(const QueryRequestTimeout *timeout);

/**
 * Primary timeout operation:
 *
 * - UNARMED and BLOCKED_CLIENT preserve the exact operation's behavior and
 *   leave the request counter unchanged.
 * - CLOCK_DEADLINE increments the request counter and checks the deadline only
 *   when it reaches QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT, then resets it to 0.
 *
 * The counter is shared by every caller using this operation and is reset when
 * BeginCycle starts a clock cycle. Because it is non-atomic, callers for the
 * same request must be serialized; this operation must not overlap a source
 * transition.
 */
bool QueryRequestTimeout_IsTimedOut(QueryRequestTimeout *timeout);

/**
 * Timeout-only synchronization between query workers and main-thread callbacks.
 * TRANSITIONAL: the completion wait remains while post-timeout draining can
 * re-enter pipeline state that is not thread-safe. Reply-state access has its
 * own synchronization and must not rely on this wait; remove the wait when the
 * drain is made thread-safe.
 */
typedef struct QueryRequestAsyncState {
  /* The CAS claim grants exclusive ownership of result production: the BG
   * winner runs the pipeline and stores results, while the timeout-callback
   * winner preempts BG and replies empty. The loser waits for completion.
   * Gated by requiresAggregateResultsSync. This claim/wait protocol can be
   * removed after the post-timeout drain is made thread-safe. */
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

/* Lifetime management.
 *
 * A QueryRequest has exactly one owner at any time — no reference counting.
 * Construction hands it to the creating flow; QueryRequest_BeginCycle hands it
 * to the blocked client (the privdata), whose OnFree ends the cycle by either
 * executing the recorded cursor disposition (PAUSE hands ownership to the
 * cursor; FREE frees through it) or freeing the request. A parked cursor owns
 * its request (Cursor.query); taking the cursor for a read hands it to the new
 * cycle. Workers and the reply/timeout callbacks borrow the privdata for the
 * cycle's duration — safe because every flow calls UnblockClient after its
 * last touch, and OnFree only runs after that. */
typedef struct QueryRequest {
  QueryRequestKind kind;
  QueryRequestArgs args;
  /* A blocked-client cycle is one initial query execution or cursor read.
   * This is set after RedisModule_BlockClient returns and cleared by OnFree;
   * per-cycle state must not be read while it is false. */
  bool blockedClientCycleActive;
  CursorInfo cursorInfo;
  RegistryInfo registryInfo;
  /* Stored results and errors written by BG before UnblockClient and consumed
   * by the main-thread reply or timeout callback. Reset at the end of each
   * cycle and again during request destruction as a safety net. */
  SafeChunkReplyState reply;
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

/** True when reply state may be accessed concurrently during this cycle. */
static inline bool QueryRequest_RequiresReplyStateSafeAccess(const QueryRequest *request) {
  return request->async.requiresAggregateResultsSync;
}

/**
 * Returns the reply state without synchronization.
 *
 * Use only when the caller has exclusive ownership of the request, or when a
 * separate synchronization event guarantees that the background thread can no
 * longer access the state.
 */
static inline ChunkReplyState *QueryRequest_GetReplyStateUnsafe(QueryRequest *request) {
  return &request->reply.state;
}

/**
 * Returns the reply state with the synchronization required by the request's
 * policy held. The caller must pair this with QueryRequest_ReleaseReplyStateSafe
 * after its final access through the returned pointer.
 *
 * Non-strict flows do not share the state concurrently, so this deliberately
 * avoids mutex overhead for them.
 */
static inline ChunkReplyState *QueryRequest_GetReplyStateSafe(QueryRequest *request) {
  // Only strict cross-thread cycles can race a background publisher with the main thread.
  if (QueryRequest_RequiresReplyStateSafeAccess(request)) {
    pthread_mutex_lock(&request->reply.lock);
  }
  return &request->reply.state;
}

/** Releases the synchronization acquired by QueryRequest_GetReplyStateSafe. */
static inline void QueryRequest_ReleaseReplyStateSafe(QueryRequest *request) {
  // The synchronization policy is stable from safe acquisition through release.
  if (QueryRequest_RequiresReplyStateSafeAccess(request)) {
    pthread_mutex_unlock(&request->reply.lock);
  }
}

/** Publishes final reply metadata and any thread-local result array safely. */
void QueryRequest_SetReplyResultsSafe(QueryRequest *request, SearchResult **results, int rc,
                                      cachedVars cv, size_t limit, const QueryError *err);

/** Replaces the stored error under the policy-dependent synchronization. */
void QueryRequest_SetReplyErrorSafe(QueryRequest *request, const QueryError *err);

/**
 * Consumes `result`. Appends it to the shared array under the reply-state lock,
 * or destroys it when the strict timeout marker has been published.
 */
bool QueryRequest_AppendReplyResultSafe(QueryRequest *request, SearchResult *result);

/**
 * Transfers the complete reply state to the caller and resets the shared state
 * under the policy-dependent synchronization.
 */
ChunkReplyState QueryRequest_TakeReplyStateSafe(QueryRequest *request);

/* Destroy the concrete request selected by `kind`. Owner-only: never call it
 * on a borrowed request (see the ownership contract on QueryRequest). */
void QueryRequest_Free(QueryRequest *request);

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

// Execution phases attribute blocked-client timeout callbacks. Other timeout
// sources do not use them, and a published timeout freezes the observed phase.
static inline void QueryRequest_SetExecutionPhase(QueryRequest *request, int phase) {
  if (request->timeout.kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT &&
      !QueryRequestTimeout_IsBlockedClientTimedOut(&request->timeout)) {
    QueryRequestAsyncState_SetExecutionPhase(&request->async, phase);
  }
}

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind,
                       const RequestConfig *requestConfig, RedisModuleString **argv,
                       uint32_t argc);
void QueryRequest_ResetReply(QueryRequest *request);
void QueryRequest_Destroy(QueryRequest *request);

#ifdef __cplusplus
}
#endif

#endif
