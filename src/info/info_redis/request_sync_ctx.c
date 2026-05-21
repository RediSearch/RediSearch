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
#include "hybrid/hybrid_request.h"

void AREQ_Free(AREQ *req);

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
    AREQ_Free(ctx->query.areq);
  } else {
    HybridRequest_Free(ctx->query.hreq);
  }
  RequestSyncCtx_Destroy(ctx);
  rm_free(ctx);
}

RequestSyncCtx *RequestSyncCtx_IncrRef(RequestSyncCtx *ctx) {
  if (ctx) {
    __atomic_fetch_add(&ctx->refcount, 1, __ATOMIC_RELAXED);
  }
  return ctx;
}

void RequestSyncCtx_DecrRef(RequestSyncCtx *ctx) {
  if (ctx && !__atomic_sub_fetch(&ctx->refcount, 1, __ATOMIC_ACQ_REL)) {
    RequestSyncCtx_Free(ctx);
  }
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

void RequestSyncCtx_ReleaseQueryRef(RequestSyncCtx *ctx) {
  RequestSyncCtx_DecrRef(ctx);
}

void RequestSyncCtx_ReleaseQueryRefCB(void *ctx) {
  RequestSyncCtx_ReleaseQueryRef((RequestSyncCtx *)ctx);
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
