/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "query_request.h"

#include <stdatomic.h>

#include "aggregate/aggregate.h"
#include "config.h"
#include "coord/rmr/chan.h"
#include "hybrid/hybrid_request.h"
#include "obfuscation/obfuscation_api.h"
#include "query_error_ffi.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "search_result_ffi.h"
#include "util/arr/arr.h"
#include "util/misc.h"
#include "util/timeout.h"

void QueryRequestTimeout_Init(QueryRequestTimeout *timeout, RSTimeoutPolicy policy,
                              long long timeoutMS) {
  // Capture the request defaults before parsing can apply command-specific overrides.
  QueryRequestTimeout_UpdateConfig(timeout, policy, timeoutMS);
  QueryRequestTimeout_Reset(timeout);
}

void QueryRequestTimeout_UpdateConfig(QueryRequestTimeout *timeout, RSTimeoutPolicy policy,
                                      long long timeoutMS) {
  RS_ASSERT(timeoutMS >= 0);
  timeout->policy = policy;
  timeout->timeoutMS = timeoutMS;
}

void QueryRequestTimeout_Reset(QueryRequestTimeout *timeout) {
  timeout->kind = QUERY_REQUEST_TIMEOUT_UNARMED;
}

void QueryRequestTimeout_BeginCycle(QueryRequestTimeout *timeout, QueryRequestTimeoutKind kind) {
  switch (kind) {
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      RS_AtomicBoolStoreRelaxed(&timeout->source.blockedClientTimedOut, false);
      timeout->kind = QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT;
      return;
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE: {
      // RETURN_STRICT depends on the blocked-client timeout callback. Clock-based
      // consumers must downgrade it to RETURN before starting their cycle.
      RS_ASSERT(timeout->policy != TimeoutPolicy_ReturnStrict);
      if (timeout->timeoutMS == 0) {
        timeout->kind = QUERY_REQUEST_TIMEOUT_UNARMED;
        return;
      }

      struct timespec duration = {
          .tv_sec = timeout->timeoutMS / 1000,
          .tv_nsec = (timeout->timeoutMS % 1000) * 1000000,
      };
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC_RAW, &now);
      rs_timeradd(&now, &duration, &timeout->source.clock.deadline);
      timeout->source.clock.counter = 0;
      timeout->kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
      return;
    }
    case QUERY_REQUEST_TIMEOUT_UNARMED:
      RS_ABORT_ALWAYS("UNARMED is not a selectable timeout source");
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

void QueryRequestTimeout_MarkTimedOut(QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  RS_AtomicBoolStoreRelaxed(&timeout->source.blockedClientTimedOut, true);
}

RS_Atomic(bool) *QueryRequestTimeout_GetBlockedClientFlag(QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  return &timeout->source.blockedClientTimedOut;
}

const struct timespec *QueryRequestTimeout_GetClockDeadline(
    const QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout->kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  return &timeout->source.clock.deadline;
}

struct timespec *QueryRequestTimeout_GetClockDeadlineForUpdate(QueryRequestTimeout *timeout) {
  if (timeout->kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    timeout->source.clock.counter = 0;
  }
  timeout->kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
  return &timeout->source.clock.deadline;
}

bool QueryRequestTimeout_IsTimedOutExact(const QueryRequestTimeout *timeout) {
  switch (timeout->kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return RS_AtomicBoolLoadRelaxed(&timeout->source.blockedClientTimedOut);
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return TimedOut(&timeout->source.clock.deadline) == TIMED_OUT;
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestTimeout_IsBlockedClientTimedOut(const QueryRequestTimeout *timeout) {
  switch (timeout->kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return QueryRequestTimeout_IsTimedOutExact(timeout);
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestTimeout_IsTimedOut(QueryRequestTimeout *timeout) {
  // Only clock reads are expensive enough to amortize. Other sources retain
  // the exact semantics of QueryRequestTimeout_IsTimedOutExact.
  if (timeout->kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    return QueryRequestTimeout_IsTimedOutExact(timeout);
  }

  RS_ASSERT(timeout->source.clock.counter < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT);
  if (RS_IsMock) {
    return false;
  }

  if (++timeout->source.clock.counter == QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT) {
    timeout->source.clock.counter = 0;
    return TimedOut(&timeout->source.clock.deadline) == TIMED_OUT;
  }
  return false;
}

static void QueryRequestArgs_Release(RedisModuleString **argv, uint32_t argc) {
  for (uint32_t ii = 0; ii < argc; ++ii) {
    RedisModule_FreeString(NULL, argv[ii]);
  }
  rm_free(argv);
}

typedef struct {
  RedisModuleString **argv;
  uint32_t argc;
} DeferredArgsRelease;

static void QueryRequestArgs_ReleaseOnMainThread(void *privdata) {
  DeferredArgsRelease *deferred = privdata;
  QueryRequestArgs_Release(deferred->argv, deferred->argc);
  rm_free(deferred);
}

static inline void ChunkReplyState_Init(ChunkReplyState *state) {
  *state = (ChunkReplyState) {0};
  state->err = QueryError_Default();
}

// Request lifecycle operations have exclusive ownership; the lock is either
// not initialized yet or has no concurrent users.
static inline void SafeChunkReplyState_InitUnsafe(SafeChunkReplyState *safeState) {
  ChunkReplyState_Init(&safeState->state);
  pthread_mutex_init(&safeState->lock, NULL);
}

static inline void SafeChunkReplyState_ResetUnsafe(SafeChunkReplyState *safeState) {
  ChunkReplyState_Destroy(&safeState->state);
  ChunkReplyState_Init(&safeState->state);
}

static inline void SafeChunkReplyState_DestroyUnsafe(SafeChunkReplyState *safeState) {
  ChunkReplyState_Destroy(&safeState->state);
  pthread_mutex_destroy(&safeState->lock);
}

void QueryRequest_SetReplyResultsSafe(QueryRequest *request, SearchResult **results, int rc,
                                      cachedVars cv, size_t limit, const QueryError *err) {
  // Publication may race the strict timeout consumer, so all fields share one critical section.
  ChunkReplyState *stored = QueryRequest_GetReplyStateSafe(request);
  // Only strict flows append incrementally; other flows publish their completed local array here.
  if (QueryRequest_RequiresReplyStateSafeAccess(request)) {
    // Strict aggregation already appended directly to the shared array.
    RS_ASSERT(results == NULL);
    if (!stored->results) {
      // A concrete empty array keeps stored-result serialization out of its live-result path.
      stored->results = array_new(SearchResult *, 8);
    }
  } else {
    // Non-synchronized flows publish their completed thread-local array once.
    stored->results = results;
  }
  stored->rc = rc;
  stored->cv = cv;
  stored->limit = limit;
  if (err) {
    QueryError_ClearError(&stored->err);
    QueryError_CloneFrom(err, &stored->err);
  }
  // Publish completion only after every field required by the consumer.
  stored->hasStoredResults = true;
  // The complete state is now published, so the strict consumer may proceed.
  QueryRequest_ReleaseReplyStateSafe(request);
}

void QueryRequest_SetReplyErrorSafe(QueryRequest *request, const QueryError *err) {
  // Error publication may race the strict timeout consumer.
  ChunkReplyState *stored = QueryRequest_GetReplyStateSafe(request);
  QueryError_ClearError(&stored->err);
  QueryError_CloneFrom(err, &stored->err);
  // The error is fully cloned, so the strict consumer may proceed.
  QueryRequest_ReleaseReplyStateSafe(request);
}

void QueryRequest_AppendReplyResultSafe(QueryRequest *request, SearchResult *result) {
  // Incremental publication is valid only for a strict cross-thread reply cycle.
  RS_ASSERT(QueryRequest_RequiresReplyStateSafeAccess(request));

  // A row already produced by the pipeline must precede the later main-thread drain.
  ChunkReplyState *stored = QueryRequest_GetReplyStateSafe(request);
  if (!stored->results) {
    stored->results = array_new(SearchResult *, 8);
  }
  array_append(stored->results, result);
  // The pointer, array header, resize, and new element are now published atomically.
  QueryRequest_ReleaseReplyStateSafe(request);
}

ChunkReplyState QueryRequest_TakeReplyStateSafe(QueryRequest *request) {
  // Ownership transfer must be atomic with respect to a strict-mode publisher.
  ChunkReplyState *stored = QueryRequest_GetReplyStateSafe(request);
  ChunkReplyState taken = *stored;
  ChunkReplyState_Init(stored);
  // Shared ownership is cleared, so serialization no longer needs the lock.
  QueryRequest_ReleaseReplyStateSafe(request);
  return taken;
}

static inline void QueryRequestAsyncState_Init(QueryRequestAsyncState *state) {
  state->requiresAggregateResultsSync = false;
  state->aggregatingResults = false;
  state->aggregateResultsClaimLost = false;
  state->aggregateResultsDone = false;
  state->safeLoadersHoldingGIL = 0;
  state->strictReadOwner = 0;
  QueryRequestAsyncState_SetExecutionPhase(state, 0);
  state->abortWakeChannel = NULL;
  pthread_mutex_init(&state->abortWakeLock, NULL);
  pthread_mutex_init(&state->aggregateResultsLock, NULL);
  pthread_cond_init(&state->aggregateResultsCond, NULL);
}

static inline void QueryRequestAsyncState_Destroy(QueryRequestAsyncState *state) {
  pthread_mutex_destroy(&state->abortWakeLock);
  pthread_mutex_destroy(&state->aggregateResultsLock);
  pthread_cond_destroy(&state->aggregateResultsCond);
}

void QueryRequest_Free(QueryRequest *request) {
  if (!request) {
    return;
  }
  switch (request->kind) {
    case QUERY_REQUEST_KIND_AREQ:
      AREQ_Free((AREQ *)request);
      return;
    case QUERY_REQUEST_KIND_HYBRID:
      HybridRequest_Free((HybridRequest *)request);
      return;
    default:
      RS_ABORT_ALWAYS("Invalid query request kind");
  }
}

static void QueryRequest_HoldArgs(QueryRequestArgs *args, RedisModuleString **argv, uint32_t argc) {
  RS_ASSERT(argv != NULL);
  RS_ASSERT(args->argv == NULL);
  args->argv = rm_malloc(argc * sizeof(*args->argv));
  args->argc = argc;
  args->parseArgc = argc;
  for (uint32_t ii = 0; ii < argc; ++ii) {
    args->argv[ii] = RedisModule_HoldString(NULL, argv[ii]);
    // Redis auto-trims retained argv after the command callback returns. Trim
    // before a worker can borrow the string storage.
    RedisModule_TrimStringAllocation(args->argv[ii]);
  }
}

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind,
                       const RequestConfig *requestConfig, RedisModuleString **argv,
                       uint32_t argc) {
  RS_ASSERT(requestConfig);
  request->kind = kind;
  request->args = (QueryRequestArgs) {
    .queryOffset = QUERY_OFFSET_NONE,
  };
  request->blockedClientCycleActive = false;
  request->cursorInfo = (CursorInfo) {0};
  request->registryInfo = (RegistryInfo) {0};
  // Initialization has exclusive ownership and must create the mutex before safe access.
  SafeChunkReplyState_InitUnsafe(&request->reply);
  QueryRequest_SetUseReplyCallback(request, false);
  QueryRequestTimeout_Init(&request->timeout, requestConfig->timeoutPolicy,
                           requestConfig->queryTimeoutMS);
  QueryRequestTimeout_BeginCycle(&request->timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestAsyncState_Init(&request->async);
  QueryRequest_SetEndProcRef(request, NULL);
  if (argv) {
    QueryRequest_HoldArgs(&request->args, argv, argc);
  } else {
    RS_ASSERT(argc == 0);
  }
}

void QueryRequest_ResetReply(QueryRequest *request) {
  // Cycle reset runs only after all worker and callback borrows have ended.
  SafeChunkReplyState_ResetUnsafe(&request->reply);
}

void QueryRequest_Destroy(QueryRequest *request) {
  // Registration is strictly per-cycle; a request still linked here would
  // leave a dangling registry entry.
  RS_ASSERT(!RegistryInfo_IsLinked(&request->registryInfo));
  // Destruction has exclusive ownership after all worker and callback borrows have ended.
  SafeChunkReplyState_DestroyUnsafe(&request->reply);
  QueryRequestAsyncState_Destroy(&request->async);
  QueryRequest_SetEndProcRef(request, NULL);
  if (request->args.argv) {
    if (MainThread_Is()) {
      QueryRequestArgs_Release(request->args.argv, request->args.argc);
    } else {
      DeferredArgsRelease *deferred = rm_new(DeferredArgsRelease);
      *deferred = (DeferredArgsRelease) {
        .argv = request->args.argv,
        .argc = request->args.argc,
      };
      int rc = RedisModule_EventLoopAddOneShot(QueryRequestArgs_ReleaseOnMainThread, deferred);
      RS_ASSERT(rc == REDISMODULE_OK);
      (void)rc;
    }
    request->args = (QueryRequestArgs) {.queryOffset = QUERY_OFFSET_NONE};
  }
}

const char *QueryRequest_ReportIndexName(const QueryRequest *request, char *obfuscated_buffer) {
  if (request->args.argc < 2) {
    return "n/a";
  }
  // The request's held argv mirrors the logical command — argv[0] is the
  // command, argv[1] the index as the caller addressed it (an alias included).
  // Plain reads and pure hashing only: this also runs in the crash handler's
  // signal context.
  size_t len;
  const char *name = RedisModule_StringPtrLen(request->args.argv[1], &len);
  if (!RSGlobalConfig.hideUserDataFromLog) {
    return name;
  }
  // Same derivation as the spec's own obfuscated name (sha1 of the name), so
  // crash entries correlate with the rest of the log unless addressed by
  // alias.
  Sha1 sha1;
  Sha1_Compute(name, len, &sha1);
  Obfuscate_Index(&sha1, obfuscated_buffer);
  return obfuscated_buffer;
}

void QueryRequestAsyncState_RegisterAbortWakeChannel(QueryRequestAsyncState *state,
                                                     struct MRChannel *channel) {
  pthread_mutex_lock(&state->abortWakeLock);
  state->abortWakeChannel = channel;
  pthread_mutex_unlock(&state->abortWakeLock);
}

void QueryRequestAsyncState_UnregisterAbortWakeChannel(QueryRequestAsyncState *state) {
  pthread_mutex_lock(&state->abortWakeLock);
  state->abortWakeChannel = NULL;
  pthread_mutex_unlock(&state->abortWakeLock);
}

void QueryRequestAsyncState_WakeAbortChannel(QueryRequestAsyncState *state) {
  pthread_mutex_lock(&state->abortWakeLock);
  if (state->abortWakeChannel) {
    MRChannel_WakeAbort(state->abortWakeChannel);
  }
  pthread_mutex_unlock(&state->abortWakeLock);
}
