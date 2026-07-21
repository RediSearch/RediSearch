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
#include "cursor.h"
#include "info/global_stats.h"

typedef struct QueryError QueryError;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Coordinator request context - wrapper for AREQ/HybridRequest that enables
 * coordinator-level timeout handling using the reply_callback pattern.
 *
 * Both AREQ and HybridRequest use the reply_callback pattern:
 * - Background thread executes query and stores results
 * - Background thread calls UnblockClient to trigger reply_callback on main thread
 * - reply_callback builds and sends the reply
 * - Timeout callback sets timedOut flag and replies with timeout error
 */
typedef struct CoordRequestCtx {
  CommandType type;
  union {
    AREQ *areq;
    HybridRequest *hreq;
  };
  _Atomic(bool) timedOut;       // Coordinator-level timeout flag
  // Execution-phase marker (QueryTimeoutStage values) covering the window before
  // the request object exists: QUEUE while waiting in the coord pool, PIPELINE once
  // a coord thread starts executing. Once the request is set, its own marker takes
  // over (see CoordRequestCtx_ExecutionStage). Frozen once `timedOut` is set.
  RS_Atomic(int) execPhase;
  pthread_mutex_t setReqLock;   // Lock for request creation/setting
  // Error that occurred before AREQ/HREQ was created (e.g., index not found).
  // When using reply_callback pattern, errors must be stored here since there's
  // no request object to store them in yet. reply_callback checks this field.
  QueryError preRequestError;
  bool useReplyCallback;
  // Distinguishes coord FT.CURSOR READ on a RETURN_STRICT. Set in
  // CursorCommand before BC arming; never mutated afterwards.
  bool isCursorReadReturnStrict;
  // Timeout policy captured on the main thread at dispatch so BG and the armed
  // timeout callback agree on one value (reading RSGlobalConfig on BG races with
  // FT.CONFIG SET). Set before BC arming; never mutated afterwards.
  RSTimeoutPolicy timeoutPolicy;
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
 * Get the request from the context.
 * Returns NULL if no request is set.
 */
void *CoordRequestCtx_GetRequest(CoordRequestCtx *ctx);

// Read the coord-level execution-phase marker (mirrors RequestSyncState_GetExecutionStage).
static inline QueryTimeoutStage CoordRequestCtx_GetExecutionStage(CoordRequestCtx *ctx) {
  return (QueryTimeoutStage)RS_AtomicIntLoadRelaxed(&ctx->execPhase);
}

// The stage the coord request reached, for timeout attribution. Before the request
// object exists the coord-level marker applies (QUEUE until a coord thread picks the
// job up); afterwards the request's own marker (PIPELINE fanning out/reducing, REPLY
// handing off the reply).
static inline QueryTimeoutStage CoordRequestCtx_ExecutionStage(CoordRequestCtx *ctx) {
  void *req = CoordRequestCtx_GetRequest(ctx);
  if (!req) {
    return CoordRequestCtx_GetExecutionStage(ctx);
  }
  return ctx->type == COMMAND_HYBRID ? HybridRequest_ExecutionStage((HybridRequest *)req)
                                     : AREQ_ExecutionStage((AREQ *)req);
}

/**
 * Check if the coordinator request has timed out.
 */
static inline bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx) {
  return RS_AtomicBoolLoadRelaxed(&ctx->timedOut);
}

// Advance the coordinator-level execution-phase marker, used until the request
// object exists. Frozen once `timedOut` is set, so the stage a timeout callback
// reads cannot change underneath it.
static inline void CoordRequestCtx_SetExecutionStage(CoordRequestCtx *ctx,
                                                     QueryTimeoutStage stage) {
  if (CoordRequestCtx_TimedOut(ctx)) {
    return;
  }
  RS_AtomicIntStoreRelaxed(&ctx->execPhase, (int)stage);
}

// Record this coordinator request's blocked-client timeout into the per-stage
// breakdown, at the stage the deadline caught it. Must be called exactly once per
// blocked-client timeout callback, after CoordRequestCtx_SetTimedOut (which freezes
// the stage markers).
static inline void CoordRequestCtx_RecordTimeoutStage(CoordRequestCtx *ctx, bool isError) {
  QueryTimeoutStageStats_Record(CoordRequestCtx_ExecutionStage(ctx), isError, COORD_ERR_WARN);
}

/**
 * Set the timeout flag on the coordinator request context.
 * Also propagates to the underlying request if set.
 */
void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx);

void CoordRequestCtx_SetUseReplyCallback(CoordRequestCtx *ctx, bool useReplyCallback);

/**
 * Mark/query this context as backing a coordinator FT.CURSOR READ on a
 * RETURN_STRICT cursor. Set once in CursorCommand before BC arming.
 */
void CoordRequestCtx_SetCursorReadReturnStrict(CoordRequestCtx *ctx, bool value);
bool CoordRequestCtx_IsCursorReadReturnStrict(CoordRequestCtx *ctx);

/** Store/read the timeout policy captured on the main thread at dispatch. */
void CoordRequestCtx_SetTimeoutPolicy(CoordRequestCtx *ctx, RSTimeoutPolicy policy);
RSTimeoutPolicy CoordRequestCtx_GetTimeoutPolicy(CoordRequestCtx *ctx);

/**
 * Store error for reply_callback to handle (pre-request errors).
 * Used when errors occur before AREQ/HREQ is created.
 * If already timed out, just clears the error.
 */
void CoordRequestCtx_ReplyOrStoreError(CoordRequestCtx *ctx, RedisModuleCtx *redisCtx, QueryError *status);

#ifdef __cplusplus
}
#endif
