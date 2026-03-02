/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include <stdatomic.h>
#include <pthread.h>

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
  _Atomic(bool) timedOut;       // Coordinator-level timeout flag
  pthread_mutex_t setReqLock;   // Lock for request creation/setting
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
void CoordRequestCtx_Free(CoordRequestCtx *ctx);

/**
 * Lock for request creation. Must be held while creating and setting the request.
 * Background thread: lock -> check timedOut -> create request -> set request -> unlock
 * Timeout callback: lock -> set timedOut -> check HasRequest -> unlock -> handle
 */
void CoordRequestCtx_LockSetRequest(CoordRequestCtx *ctx);
void CoordRequestCtx_UnlockSetRequest(CoordRequestCtx *ctx);

/**
 * Set the request pointer and take shared ownership.
 * Called by background thread after creating the request, while holding the lock.
 *
 * This function increments the request's refcount, establishing shared ownership
 * between the background thread (which created the request) and the CoordRequestCtx
 * (which may be freed by the timeout callback). Both sides must call DecrRef when done.
 */
void CoordRequestCtx_SetRequest(CoordRequestCtx *ctx, void *req);

/**
 * Check if the request pointer has been set.
 */
bool CoordRequestCtx_HasRequest(CoordRequestCtx *ctx);

/**
 * Check if the coordinator request has timed out.
 */
bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx);

/**
 * Set the timeout flag on the coordinator request context.
 * Also propagates to the underlying request if set.
 */
void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx);

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
