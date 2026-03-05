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
  ctx->preRequestError = QueryError_Default();
  return ctx;
}

void CoordRequestCtx_Free(CoordRequestCtx *ctx) {
  if (!ctx) return;

  // Clear pre-request error if set
  QueryError_ClearError(&ctx->preRequestError);

  // Decrement refcount on the request (if set)
  if (ctx->type == COMMAND_HYBRID) {
    if (ctx->hreq) HybridRequest_DecrRef(ctx->hreq);
  } else {
    if (ctx->areq) AREQ_DecrRef(ctx->areq);
  }

  rm_free(ctx);
}

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

void *CoordRequestCtx_GetRequest(CoordRequestCtx *ctx) {
  return ctx->type == COMMAND_HYBRID ? ctx->hreq : ctx->areq;
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

void CoordRequestCtx_SetUseReplyCallback(CoordRequestCtx *ctx, bool useReplyCallback) {
  ctx->useReplyCallback = useReplyCallback;
}

void CoordRequestCtx_ReplyOrStoreError(CoordRequestCtx *req, RedisModuleCtx *ctx, QueryError *status) {
  if (req->useReplyCallback) {
    // Deep copy since QueryError contains heap-allocated strings.
    QueryError_CloneFrom(status, &req->preRequestError);
    // Clear the original to avoid leaking heap-allocated strings.
    QueryError_ClearError(status);
  } else {
    QueryError_ReplyAndClear(ctx, status);
  }
}
