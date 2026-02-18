/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "coord_request_ctx.h"
#include "rmalloc.h"
#include <stdatomic.h>

CoordRequestCtx *CoordRequestCtx_New(CommandType type) {
  CoordRequestCtx *ctx = rm_calloc(1, sizeof(CoordRequestCtx));
  ctx->type = type;

  // Initialize timeout coordination fields only - real init happens on background thread
  if (type == COMMAND_HYBRID) {
    atomic_store_explicit(&ctx->hreq.timedOut, false, memory_order_relaxed);
    atomic_store_explicit(&ctx->hreq.replyState, ReplyState_NotReplied, memory_order_relaxed);
    ctx->hreq.refcount = 1;
  } else {
    atomic_store_explicit(&ctx->areq.timedOut, false, memory_order_relaxed);
    atomic_store_explicit(&ctx->areq.replyState, ReplyState_NotReplied, memory_order_relaxed);
    ctx->areq.refcount = 1;
  }

  return ctx;
}

void CoordRequestCtx_Free(CoordRequestCtx *ctx) {
  if (!ctx) return;

  // TODO: Cleanup embedded AREQ/HybridRequest fields
  // This will be implemented when we integrate with the background thread flow

  rm_free(ctx);
}

bool CoordRequestCtx_TryClaimReply(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return HybridRequest_TryClaimReply(&ctx->hreq);
  } else {
    // AREQ's TryClaimReply is a static inline in aggregate_exec.c, so we implement here
    int expected = ReplyState_NotReplied;
    return atomic_compare_exchange_strong_explicit(&ctx->areq.replyState, &expected,
        ReplyState_Replying, memory_order_acq_rel, memory_order_acquire);
  }
}

void CoordRequestCtx_MarkReplied(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    HybridRequest_MarkReplied(&ctx->hreq);
  } else {
    // AREQ's MarkReplied is a static inline in aggregate_exec.c, so we implement here
    atomic_store_explicit(&ctx->areq.replyState, ReplyState_Replied, memory_order_release);
  }
}

uint8_t CoordRequestCtx_GetReplyState(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return HybridRequest_GetReplyState(&ctx->hreq);
  } else {
    return atomic_load_explicit(&ctx->areq.replyState, memory_order_acquire);
  }
}
