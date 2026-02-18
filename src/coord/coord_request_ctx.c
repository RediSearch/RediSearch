/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "coord_request_ctx.h"
#include "rmalloc.h"

CoordRequestCtx *CoordRequestCtx_New(CommandType type) {
  CoordRequestCtx *ctx = rm_calloc(1, sizeof(CoordRequestCtx));
  ctx->type = type;
  // Request pointer starts as NULL, set by background thread after parsing
  return ctx;
}

void CoordRequestCtx_Free(void *ptr) {
  CoordRequestCtx *ctx = ptr;
  if (!ctx) return;

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
