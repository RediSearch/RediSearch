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
#include "info/global_stats.h"
#include "cursor.h"

#define COORD_REQUEST_CTX_UNSUPPORTED_TYPE() \
  RS_LOG_ASSERT(false, "CoordRequestCtx only supports COMMAND_AGGREGATE and COMMAND_HYBRID")

CoordRequestCtx *CoordRequestCtx_New(CommandType type) {
  CoordRequestCtx *ctx = rm_calloc(1, sizeof(CoordRequestCtx));
  ctx->type = type;
  atomic_store_explicit(&ctx->timedOut, false, memory_order_relaxed);
  pthread_mutex_init(&ctx->setReqLock, NULL);
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
  } else if (ctx->type == COMMAND_AGGREGATE) {
    if (ctx->areq) {
      // Timeout edge case for cursor queries with useReplyCallback:
      // When timeout fires before reply_callback runs, but after the cursor was created and
      // stored in areq->storedReplyState.cursor, the cursor needs to be freed manually.
      AREQ_CleanUpStoredCursor(ctx->areq);
      AREQ_DecrRef(ctx->areq);
    }
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
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

void CoordRequestCtx_SetRequest(CoordRequestCtx *ctx, void *req) {
  if (ctx->type == COMMAND_HYBRID) {
    ctx->hreq = HybridRequest_IncrRef((HybridRequest *)req);
  } else if (ctx->type == COMMAND_AGGREGATE) {
    ctx->areq = AREQ_IncrRef((AREQ *)req);
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
  }

  // Propagate useReplyCallback to the request
  if (ctx->type == COMMAND_HYBRID) {
    ((HybridRequest *)req)->useReplyCallback = ctx->useReplyCallback;
  } else if (ctx->type == COMMAND_AGGREGATE) {
    ((AREQ *)req)->useReplyCallback = ctx->useReplyCallback;
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
  }

  // Propagate timeout to the request if already set
  if (CoordRequestCtx_TimedOut(ctx)) {
    CoordRequestCtx_SetTimedOut(ctx);
  }
}

bool CoordRequestCtx_HasRequest(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return ctx->hreq != NULL;
  } else if (ctx->type == COMMAND_AGGREGATE) {
    return ctx->areq != NULL;
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
    return false;
  }
}

void *CoordRequestCtx_GetRequest(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return (void *)ctx->hreq;
  } else if (ctx->type == COMMAND_AGGREGATE) {
    return (void *)ctx->areq;
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
    return NULL;
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
  } else if (ctx->type == COMMAND_AGGREGATE) {
    if (ctx->areq) AREQ_SetTimedOut(ctx->areq);
  } else {
    COORD_REQUEST_CTX_UNSUPPORTED_TYPE();
  }
}

void CoordRequestCtx_SetUseReplyCallback(CoordRequestCtx *ctx, bool useReplyCallback) {
  ctx->useReplyCallback = useReplyCallback;
}

void CoordRequestCtx_ReplyOrStoreError(CoordRequestCtx *req, RedisModuleCtx *ctx, QueryError *status) {
  if (req->useReplyCallback) {
    // Assert no existing error
    RS_ASSERT(!QueryError_HasError(&req->preRequestError));

    // Deep copy since QueryError contains heap-allocated strings.
    QueryError_CloneFrom(status, &req->preRequestError);
    // Clear the original to avoid leaking heap-allocated strings.
    QueryError_ClearError(status);
  } else {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, COORD_ERR_WARN);
    QueryError_ReplyAndClear(ctx, status);
  }
}
