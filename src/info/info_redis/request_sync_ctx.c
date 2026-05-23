/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "aggregate/aggregate.h"
#include "coord/rmr/chan.h"
#include "cursor.h"
#include "hybrid/hybrid_request.h"
#include "search_result_ffi.h"

void ChunkReplyState_Destroy(ChunkReplyState *state) {
  // Free any stored results that weren't consumed
  // (e.g., if timeout occurred before reply_callback ran)
  if (state->results) {
    for (size_t i = 0; i < array_len(state->results); i++) {
      SearchResult_Destroy(state->results[i]);
      rm_free(state->results[i]);
    }
    array_free(state->results);
    state->results = NULL;
  }

  // Timeout edge case: cursor wasn't handled by reply_callback.
  // See ChunkReplyState ownership model in aggregate.h for full explanation.
  // The RSC free path owns the query, so clear cursor->query before freeing
  // the cursor to avoid re-entering RequestSyncCtx_Free from Cursor_Free.
  if (state->cursor) {
    state->cursor->query = NULL;
    Cursor_Free(state->cursor);
    state->cursor = NULL;
  }

  // Clear stored error state
  QueryError_ClearError(&state->err);
}

RequestSyncCtx *RequestSyncCtx_NewAREQ(AREQ *areq) {
  RequestSyncCtx *ctx = rm_new(RequestSyncCtx);
  RequestSyncCtx_Init(ctx, REQUEST_KIND_AREQ, areq);
  return ctx;
}

RequestSyncCtx *RequestSyncCtx_NewHybrid(HybridRequest *hreq) {
  RequestSyncCtx *ctx = rm_new(RequestSyncCtx);
  RequestSyncCtx_Init(ctx, REQUEST_KIND_HYBRID, hreq);
  return ctx;
}

void RequestSyncCtx_Free(RequestSyncCtx *ctx) {
  if (!ctx) {
    return;
  }
  ChunkReplyState_Destroy(&ctx->storedReplyState);
  if (ctx->kind == REQUEST_KIND_AREQ) {
    AREQ_FreeFromRequestSyncCtx(ctx->query.areq);
  } else {
    HybridRequest_Free(ctx->query.hreq);
  }
  RequestSyncCtx_Destroy(ctx);
  rm_free(ctx);
}

AREQ *RequestSyncCtx_GetAREQ(RequestSyncCtx *ctx) {
  return (ctx && ctx->kind == REQUEST_KIND_AREQ) ? ctx->query.areq : NULL;
}

HybridRequest *RequestSyncCtx_GetHybridRequest(RequestSyncCtx *ctx) {
  return (ctx && ctx->kind == REQUEST_KIND_HYBRID) ? ctx->query.hreq : NULL;
}

AREQ *RequestSyncCtx_GetCursorAREQ(RequestSyncCtx *ctx, uint64_t cursorId) {
  AREQ *areq = RequestSyncCtx_GetAREQ(ctx);
  if (areq) {
    return areq;
  }

  HybridRequest *hreq = RequestSyncCtx_GetHybridRequest(ctx);
  if (!hreq) {
    return NULL;
  }

  for (size_t i = 0; i < hreq->nrequests; i++) {
    AREQ *subquery = hreq->requests[i];
    if (subquery->cursor_id == cursorId) {
      return subquery;
    }
  }
  return NULL;
}

bool RequestSyncCtx_UseReplyCallback(RequestSyncCtx *ctx) {
  return ctx && ctx->useReplyCallback;
}

void RequestSyncCtx_SetUseReplyCallback(RequestSyncCtx *ctx, bool useReplyCallback) {
  if (!ctx) {
    return;
  }
  ctx->useReplyCallback = useReplyCallback;
}

ChunkReplyState *RequestSyncCtx_GetReplyState(RequestSyncCtx *ctx) {
  return ctx ? &ctx->storedReplyState : NULL;
}

void RequestSyncCtx_RegisterAbortWakeChannel(RequestSyncCtx *ctx, struct MRChannel *chan) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  ctx->abortWakeChannel = chan;
  pthread_mutex_unlock(&ctx->abortWakeLock);
}

void RequestSyncCtx_UnregisterAbortWakeChannel(RequestSyncCtx *ctx) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  ctx->abortWakeChannel = NULL;
  pthread_mutex_unlock(&ctx->abortWakeLock);
}

void RequestSyncCtx_WakeAbortChannel(RequestSyncCtx *ctx) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  if (ctx->abortWakeChannel) {
    MRChannel_WakeAbort(ctx->abortWakeChannel);
  }
  pthread_mutex_unlock(&ctx->abortWakeLock);
}
