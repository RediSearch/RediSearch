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
#include "module.h"
#include "obfuscation/obfuscation_api.h"
#include "query_error_ffi.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/misc.h"
#include "util/timeout.h"

static void QueryRequestLimits_ClearMemoryStop(QueryRequestLimits *limits) {
  RS_AtomicBoolStoreRelaxed(&limits->memory.stopped, false);
}

void QueryRequestLimits_Init(QueryRequestLimits *limits, const TimeoutConfig *config) {
  RS_ASSERT(config);
  RS_ASSERT(config->queryTimeoutMS >= 0);
  limits->timeout.config = *config;
  QueryRequestLimits_ClearMemoryStop(limits);
  QueryRequestLimits_Reset(limits);
}

void QueryRequestLimits_Reset(QueryRequestLimits *limits) {
  limits->timeout.kind = QUERY_REQUEST_TIMEOUT_UNARMED;
}

void QueryRequestLimits_BeginCycle(QueryRequestLimits *limits, QueryRequestTimeoutKind kind) {
  QueryRequestLimits_ClearMemoryStop(limits);
  switch (kind) {
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      RS_AtomicBoolStoreRelaxed(&limits->timeout.source.blockedClientTimedOut, false);
      limits->timeout.kind = QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT;
      return;
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE: {
      // RETURN_STRICT depends on the blocked-client timeout callback. Clock-based
      // consumers must downgrade it to RETURN before starting their cycle.
      RS_ASSERT(limits->timeout.config.timeoutPolicy != TimeoutPolicy_ReturnStrict);
      if (limits->timeout.config.queryTimeoutMS == 0) {
        limits->timeout.kind = QUERY_REQUEST_TIMEOUT_UNARMED;
        return;
      }

      struct timespec duration = {
          .tv_sec = limits->timeout.config.queryTimeoutMS / 1000,
          .tv_nsec = (limits->timeout.config.queryTimeoutMS % 1000) * 1000000,
      };
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC_RAW, &now);
      rs_timeradd(&now, &duration, &limits->timeout.source.clock.deadline);
      limits->timeout.source.clock.counter = 0;
      limits->timeout.kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
      return;
    }
    case QUERY_REQUEST_TIMEOUT_UNARMED:
      RS_ABORT_ALWAYS("UNARMED is not a selectable timeout source");
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

void QueryRequestLimits_MarkTimedOut(QueryRequestLimits *limits) {
  RS_ASSERT(limits->timeout.kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  RS_AtomicBoolStoreRelaxed(&limits->timeout.source.blockedClientTimedOut, true);
}

void QueryRequestLimits_RequestMemoryStop(QueryRequestLimits *limits) {
  RS_AtomicBoolStoreRelaxed(&limits->memory.stopped, true);
}

RS_Atomic(bool) *QueryRequestLimits_GetMemoryStopFlag(QueryRequestLimits *limits) {
  return &limits->memory.stopped;
}

RS_Atomic(bool) *QueryRequestLimits_GetBlockedClientFlag(QueryRequestLimits *limits) {
  RS_ASSERT(limits->timeout.kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  return &limits->timeout.source.blockedClientTimedOut;
}

const struct timespec *QueryRequestLimits_GetClockDeadline(
    const QueryRequestLimits *limits) {
  RS_ASSERT(limits->timeout.kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  return &limits->timeout.source.clock.deadline;
}

struct timespec *QueryRequestLimits_GetClockDeadlineForUpdate(QueryRequestLimits *limits) {
  if (limits->timeout.kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    limits->timeout.source.clock.counter = 0;
  }
  limits->timeout.kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
  return &limits->timeout.source.clock.deadline;
}

bool QueryRequestLimits_IsTimedOutExact(const QueryRequestLimits *limits) {
  switch (limits->timeout.kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return RS_AtomicBoolLoadRelaxed(&limits->timeout.source.blockedClientTimedOut);
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return TimedOut(&limits->timeout.source.clock.deadline) == TIMED_OUT;
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestLimits_IsBlockedClientTimedOut(const QueryRequestLimits *limits) {
  switch (limits->timeout.kind) {
    case QUERY_REQUEST_TIMEOUT_UNARMED:
    case QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE:
      return false;
    case QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT:
      return QueryRequestLimits_IsTimedOutExact(limits);
  }
  RS_ABORT_ALWAYS("Invalid query timeout kind");
}

bool QueryRequestTimeout_IsTimedOutOrStopped(const QueryRequestTimeout *timeout) {
  return QueryRequestTimeout_IsStopRequested(timeout) ||
         QueryRequestTimeout_IsTimedOutExact(timeout);
}

bool QueryRequestTimeout_IsTimedOut(QueryRequestTimeout *timeout) {
  // Only clock reads are expensive enough to amortize. Other sources retain
  // the exact semantics of QueryRequestLimits_IsTimedOutExact.
  if (limits->timeout.kind != QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
    return QueryRequestLimits_IsTimedOutExact(limits);
  }

  RS_ASSERT(limits->timeout.source.clock.counter < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT);
  if (RS_IsMock) {
    return false;
  }

  if (++limits->timeout.source.clock.counter == QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT) {
    limits->timeout.source.clock.counter = 0;
    return TimedOut(&limits->timeout.source.clock.deadline) == TIMED_OUT;
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

bool QueryRequest_TryClaimResults(QueryRequest *request) {
  bool expected = false;
  return atomic_compare_exchange_strong_explicit(&request->async.aggregatingResults, &expected,
                                                 true, memory_order_relaxed, memory_order_relaxed);
}

void QueryRequest_SignalResultsComplete(QueryRequest *request) {
  pthread_mutex_lock(&request->async.aggregateResultsLock);
  request->async.aggregateResultsDone = true;
  // A request has at most one timeout callback waiting for result production.
  pthread_cond_signal(&request->async.aggregateResultsCond);
  pthread_mutex_unlock(&request->async.aggregateResultsLock);
}

void QueryRequest_WaitForResultsComplete(QueryRequest *request) {
  pthread_mutex_lock(&request->async.aggregateResultsLock);
  while (!request->async.aggregateResultsDone) {
    pthread_cond_wait(&request->async.aggregateResultsCond, &request->async.aggregateResultsLock);
  }
  pthread_mutex_unlock(&request->async.aggregateResultsLock);
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
    case QUERY_REQUEST_KIND_COORD_SEARCH:
      searchRequestCtx_Free((searchRequestCtx *)request);
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

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind, const RequestConfig *requestConfig,
                       const TimeoutConfig *timeoutConfig, RedisModuleString **argv, uint32_t argc) {
  RS_ASSERT(requestConfig);
  request->kind = kind;
  request->reqConfig = *requestConfig;
  request->cursorConfig = (CursorConfig){0};
  request->args = (QueryRequestArgs) {
    .queryOffset = QUERY_OFFSET_NONE,
  };
  request->blockedClientCycleActive = false;
  request->cursorInfo = (CursorInfo) {0};
  request->registryInfo = (RegistryInfo) {0};
  ChunkReplyState_Init(&request->reply);
  request->replyDeferred = false;
#ifdef ENABLE_ASSERT
  request->inlineReplyCount = 0;
#endif
  RS_ASSERT(timeoutConfig);
  QueryRequestLimits_Init(&request->limits, timeoutConfig);
  QueryRequestLimits_BeginCycle(&request->limits, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
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
  // Registration is strictly per-cycle; a request still linked here would
  // leave a dangling registry entry.
  RS_ASSERT(!RegistryInfo_IsLinked(&request->registryInfo));
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
