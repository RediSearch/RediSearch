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
 * This struct is allocated on the main thread before dispatching to background thread.
 * It embeds either AREQ or HybridRequest directly to enable:
 * - Single allocation (wrapper + request in one block)
 * - Immediate access to timeout fields from main thread
 * - Background thread fills in remaining fields after parsing
 *
 * The timeout fields (timedOut, replyState, refcount) in the embedded AREQ/HybridRequest
 * enable synchronization between main thread (timeout callback) and background thread.
 */
typedef struct CoordRequestCtx {
  CommandType type;
  union {
    AREQ areq;
    HybridRequest hreq;
  };
} CoordRequestCtx;

/**
 * Allocate and minimally initialize a CoordRequestCtx on the main thread.
 * Only initializes timeout coordination fields. Real request initialization
 * happens on the background thread via AREQ_Init/HybridRequest_Init.
 */
CoordRequestCtx *CoordRequestCtx_New(CommandType type);

/**
 * Free the CoordRequestCtx and cleanup embedded AREQ/HybridRequest fields.
 */
void CoordRequestCtx_Free(CoordRequestCtx *ctx);

/**
 * Check if the request has timed out.
 */
static inline bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    return HybridRequest_TimedOut(&ctx->hreq);
  } else {
    return AREQ_TimedOut(&ctx->areq);
  }
}

/**
 * Set the timeout flag on the embedded request.
 */
static inline void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx) {
  if (ctx->type == COMMAND_HYBRID) {
    HybridRequest_SetTimedOut(&ctx->hreq);
  } else {
    AREQ_SetTimedOut(&ctx->areq);
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
