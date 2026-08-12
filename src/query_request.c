/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "query_request.h"

#include "coord/rmr/chan.h"
#include "query_error_ffi.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/misc.h"

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

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind, RedisModuleString **argv,
                       uint32_t argc) {
  request->kind = kind;
  request->args = (QueryRequestArgs) {
    .queryOffset = QUERY_OFFSET_NONE,
  };
  request->blockedClientCycleActive = false;
  request->cursorInfo = (CursorInfo) {0};
  request->registryInfo = (RegistryInfo) {0};
  ChunkReplyState_Init(&request->reply);
  QueryRequestTimeout_ClearTimedOut(&request->timeout);
  request->timeout.skipTimeoutChecks = false;
  QueryRequestAsyncState_Init(&request->async);
  QueryRequest_SetEndProcRef(request, NULL);
  if (argv) {
    QueryRequest_HoldArgs(request, argv, argc);
  } else {
    RS_ASSERT(argc == 0);
  }
}

void QueryRequest_HoldArgs(QueryRequest *request, RedisModuleString **argv, uint32_t argc) {
  RS_ASSERT(argv != NULL);
  RS_ASSERT(request->args.argv == NULL);
  request->args.argv = rm_malloc(argc * sizeof(*request->args.argv));
  request->args.argc = argc;
  request->args.parseArgc = argc;
  for (uint32_t ii = 0; ii < argc; ++ii) {
    request->args.argv[ii] = RedisModule_HoldString(NULL, argv[ii]);
    // Redis auto-trims retained argv after the command callback returns. Trim
    // before a worker can borrow the string storage.
    RedisModule_TrimStringAllocation(request->args.argv[ii]);
  }
}

void QueryRequest_ResetReply(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
  ChunkReplyState_Init(&request->reply);
}

void QueryRequest_Destroy(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
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
