/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Coordinator request context - wrapper for AREQ/HybridRequest that enables
 * coordinator-level timeout handling.
 *
 * Holds a pointer to the actual request (AREQ or HybridRequest), which is
 * managed via reference counting. The background thread creates and initializes
 * the request, then sets the pointer here for timeout coordination.
 *
 * The timeout fields (timedOut, replyState, refcount) in the pointed-to request
 * enable synchronization between main thread (timeout callback) and background thread.
 */
typedef struct CoordRequestCtx {
  CommandType type;
  union {
    AREQ *areq;
    HybridRequest *hreq;
  };
} CoordRequestCtx;

/**
 * Allocate a CoordRequestCtx with NULL request pointer.
 * The request pointer is set later by the background thread after parsing.
 */
CoordRequestCtx *CoordRequestCtx_New(CommandType type);

/**
 * Free the CoordRequestCtx and decrement the request's refcount.
 * Takes void* to be compatible with free_privdata callback signature.
 */
void CoordRequestCtx_Free(void *ctx);

/**
 * Set the request pointer and take shared ownership.
 * Called by background thread after creating the request.
 *
 * This function increments the request's refcount, establishing shared ownership
 * between the background thread (which created the request) and the CoordRequestCtx
 * (which may be freed by the timeout callback). Both sides must call DecrRef when done.
 */
void CoordRequestCtx_SetRequest(CoordRequestCtx *ctx, void *req);

/**
 * Check if the request has timed out.
 */
static inline bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return ctx->hreq ? HybridRequest_TimedOut(ctx->hreq) : false;
  } else {
    return ctx->areq ? AREQ_TimedOut(ctx->areq) : false;
  }
}

/**
 * Set the timeout flag on the request.
 */
static inline void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    if (ctx->hreq) HybridRequest_SetTimedOut(ctx->hreq);
  } else {
    if (ctx->areq) AREQ_SetTimedOut(ctx->areq);
  }
}

/**
 * Try to claim reply ownership. Returns true if claimed (state was NOT_REPLIED),
 * false if already claimed or replied (state was REPLYING or REPLIED).
 */
bool CoordRequestCtx_TryClaimReply(CoordRequestCtx *ctx);

/**
 * Mark reply as complete. Must only be called after successfully claiming reply.
 */
void CoordRequestCtx_MarkReplied(CoordRequestCtx *ctx);

/**
 * Get current reply state (for checking/waiting in timeout callback).
 */
uint8_t CoordRequestCtx_GetReplyState(CoordRequestCtx *ctx);

#ifdef __cplusplus
}
#endif
