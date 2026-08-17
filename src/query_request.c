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
#include "coord/rmr/chan.h"
#include "hybrid/hybrid_request.h"
#include "query_error_ffi.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
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
      rs_timeradd(&now, &duration, &timeout->source.clockDeadline);
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
  return &timeout->source.clockDeadline;
}

struct timespec *QueryRequestTimeout_GetClockDeadlineForUpdate(QueryRequestTimeout *timeout) {
  timeout->kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
  return &timeout->source.clockDeadline;
}

bool QueryRequestTimeout_IsTimedOut(const QueryRequestTimeout *timeout) {
  switch (timeout->kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return RS_AtomicBoolLoadRelaxed(&timeout->source.blockedClientTimedOut);
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return TimedOut(&timeout->source.clockDeadline) == TIMED_OUT;
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestTimeout_IsBlockedClientTimedOut(const QueryRequestTimeout *timeout) {
  switch (timeout->kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return QueryRequestTimeout_IsTimedOut(timeout);
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestTimeout_IsBlockedClientTimedOutCallback(void *arg) {
  const QueryRequestTimeout *timeout = arg;
  return timeout && QueryRequestTimeout_IsBlockedClientTimedOut(timeout);
}

bool QueryRequestTimeout_IsTimedOutWithCounter(const QueryRequestTimeout *timeout,
                                               uint32_t *counter) {
  // Only clock reads are expensive enough to amortize. Other sources retain
  // the exact semantics of QueryRequestTimeout_IsTimedOut.
  if (timeout->kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    return QueryRequestTimeout_IsTimedOut(timeout);
  }

  RS_ASSERT(counter && *counter < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT);
  if (RS_IsMock) {
    return false;
  }

  if (++(*counter) == QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT) {
    *counter = 0;
    return QueryRequestTimeout_IsTimedOut(timeout);
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

QueryRequest *QueryRequest_IncrRef(QueryRequest *request) {
  atomic_fetch_add_explicit(&request->refcount, 1, memory_order_relaxed);
  return request;
}

void QueryRequest_DecrRef(QueryRequest *request) {
  if (!request) {
    return;
  }
  int previous = atomic_fetch_sub_explicit(&request->refcount, 1, memory_order_acq_rel);
  RS_LOG_ASSERT_ALWAYS(previous > 0, "QueryRequest reference count underflow");
  if (previous != 1) {
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
  RS_AtomicIntStoreRelaxed(&request->refcount, 1);
  request->args = (QueryRequestArgs) {
    .queryOffset = QUERY_OFFSET_NONE,
  };
  request->blockedClientCycleActive = false;
  request->cursorInfo = (CursorInfo) {0};
  request->registryInfo = (RegistryInfo) {0};
  ChunkReplyState_Init(&request->reply);
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
  ChunkReplyState_Destroy(&request->reply);
  ChunkReplyState_Init(&request->reply);
}

void QueryRequest_Destroy(QueryRequest *request) {
  QueryRequest_ResetReply(request);
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
