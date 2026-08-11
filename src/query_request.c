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

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind) {
  request->kind = kind;
  request->blockedClientCycleActive = false;
  request->cursorInfo = (CursorInfo) {0};
  request->registryInfo = (RegistryInfo) {0};
  ChunkReplyState_Init(&request->reply);
  QueryRequestTimeout_ClearTimedOut(&request->timeout);
  QueryRequestAsyncState_Init(&request->async);
  QueryRequest_SetEndProcRef(request, NULL);
}

void QueryRequest_ResetReply(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
  ChunkReplyState_Init(&request->reply);
}

void QueryRequest_Destroy(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
  QueryRequestAsyncState_Destroy(&request->async);
  QueryRequest_SetEndProcRef(request, NULL);
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
