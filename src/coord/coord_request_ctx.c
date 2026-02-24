/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "coord_request_ctx.h"
#include "rmalloc.h"

CoordRequestCtx *CoordRequestCtx_New(CommandType type) {
  CoordRequestCtx *ctx = rm_calloc(1, sizeof(CoordRequestCtx));
  ctx->type = type;
  atomic_store_explicit(&ctx->timedOut, false, memory_order_relaxed);
  pthread_mutex_init(&ctx->setReqLock, NULL);
  // Request pointer starts as NULL, set by background thread after parsing
  return ctx;
}

void CoordRequestCtx_Free(CoordRequestCtx *ctx) {
  if (!ctx) return;

  // Decrement refcount on the request (if set)
  if (ctx->type == COMMAND_HYBRID) {
    if (ctx->hreq) HybridRequest_DecrRef(ctx->hreq);
  } else {
    if (ctx->areq) AREQ_DecrRef(ctx->areq);
  }

  pthread_mutex_destroy(&ctx->setReqLock);
  rm_free(ctx);
}

void CoordRequestCtx_LockSetRequest(CoordRequestCtx *ctx) {
  pthread_mutex_lock(&ctx->setReqLock);
}

void CoordRequestCtx_UnlockSetRequest(CoordRequestCtx *ctx) {
  pthread_mutex_unlock(&ctx->setReqLock);
}

// Must be called with setReqLock held.
void CoordRequestCtx_SetRequest(CoordRequestCtx *ctx, void *req) {
  if (ctx->type == COMMAND_HYBRID) {
    ctx->hreq = HybridRequest_IncrRef((HybridRequest *)req);
  } else {
    ctx->areq = AREQ_IncrRef((AREQ *)req);
  }
}

bool CoordRequestCtx_HasRequest(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return ctx->hreq != NULL;
  } else {
    return ctx->areq != NULL;
  }
}

bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx) {
  return atomic_load_explicit(&ctx->timedOut, memory_order_acquire);
}

void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx) {
  atomic_store_explicit(&ctx->timedOut, true, memory_order_release);
  // Also propagate to the underlying request if set
  if (ctx->type == COMMAND_HYBRID) {
    if (ctx->hreq) HybridRequest_SetTimedOut(ctx->hreq);
  } else {
    if (ctx->areq) AREQ_SetTimedOut(ctx->areq);
  }
}

bool CoordRequestCtx_TryClaimReply(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return ctx->hreq ? HybridRequest_TryClaimReply(ctx->hreq) : false;
  } else {
    return ctx->areq ? AREQ_TryClaimReply(ctx->areq) : false;
  }
}

void CoordRequestCtx_MarkReplied(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    if (ctx->hreq) HybridRequest_MarkReplied(ctx->hreq);
  } else {
    if (ctx->areq) AREQ_MarkReplied(ctx->areq);
  }
}

uint8_t CoordRequestCtx_GetReplyState(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return ctx->hreq ? HybridRequest_GetReplyState(ctx->hreq) : ReplyState_NotReplied;
  } else {
    return ctx->areq ? AREQ_GetReplyState(ctx->areq) : ReplyState_NotReplied;
  }
}
