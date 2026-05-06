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
#include "query_error.h"
#include "cursor.h"

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
  pthread_mutex_t setReqLock;   // Lock for request creation/setting
  // Error that occurred before AREQ/HREQ was created (e.g., index not found).
  // When using reply_callback pattern, errors must be stored here since there's
  // no request object to store them in yet. reply_callback checks this field.
  QueryError preRequestError;
  bool useReplyCallback;
  // True if this context backs a coordinator FT.CURSOR READ on a RETURN_STRICT
  // cursor. Distinguishes the new path from FAIL cursor reads (also use the
  // reply_callback pattern). Set in CursorCommand before BC arming; never
  // mutated afterwards.
  bool isCursorReadReturnStrict;
  // Per-read cursor handle parked here while the BG worker drives the chunk.
  // Set inside RSCursorReadCommand's setRequestLock critical section together
  // with SetRequest, so any consumer that observes a non-NULL request also
  // observes the parked cursor (or finds it already taken by a peer consumer).
  //
  // Concurrent access is serialized externally — no internal lock required
  // for the take/null race:
  //   * BG error sub-path takes + nulls *before* AREQ_SignalAggregateResultsComplete;
  //     timer's take happens after AREQ_WaitForAggregateResultsComplete returns,
  //     so the condvar provides happens-before (ordering + memory visibility).
  //   * Reply callback's take runs after BG calls RM_UnblockClient, which
  //     happens after BG returns from cursorRead (so after any BG take).
  //   * Reply and timer callbacks are mutually exclusive per Redis BC pattern.
  //   * free_privdata runs strictly after exactly one of {reply, timer, neither
  //     (disconnect)} per Redis BC pattern.
  // The loser of any apparent race observes NULL and no-ops.
  // NULL on FAIL and on every non-RETURN_STRICT-cursor-read flow.
  Cursor *parkedCursor;
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

/**
 * Check if the coordinator request has timed out.
 */
bool CoordRequestCtx_TimedOut(CoordRequestCtx *ctx);

/**
 * Set the timeout flag on the coordinator request context.
 * Also propagates to the underlying request if set.
 */
void CoordRequestCtx_SetTimedOut(CoordRequestCtx *ctx);

void CoordRequestCtx_SetUseReplyCallback(CoordRequestCtx *ctx, bool useReplyCallback);

/**
 * Mark this context as backing a coordinator FT.CURSOR READ on a RETURN_STRICT
 * cursor. Set once in CursorCommand before BC arming; consulted by the
 * RSCursorReadCommand take-lock window and the reply / timer / disconnect
 * cursor-disposition paths.
 */
void CoordRequestCtx_SetCursorReadReturnStrict(CoordRequestCtx *ctx, bool value);
bool CoordRequestCtx_IsCursorReadReturnStrict(CoordRequestCtx *ctx);

/**
 * Park a cursor handle on the context. Used inside RSCursorReadCommand's
 * setRequestLock critical section so the parked handle is published
 * atomically with the request pointer — that publication ordering is what
 * later takers rely on, not internal locking on this call.
 */
void CoordRequestCtx_SetParkedCursor(CoordRequestCtx *ctx, Cursor *cursor);

/**
 * Take ownership of the parked cursor: returns the current handle and clears
 * the field. Idempotent — returns NULL after the first successful take.
 *
 * Lockless. The four consumers (timer reply path, reply callback, BG error
 * sub-path, free_privdata on disconnect) cannot race in practice: their
 * temporal ordering is established by the condvar pair around
 * AREQ_SignalAggregateResultsComplete / AREQ_WaitForAggregateResultsComplete,
 * by RM_UnblockClient happens-before reply dispatch, and by the Redis
 * blocked-client pattern (reply XOR timer, then free_privdata). See the
 * `parkedCursor` field comment in this header for the full ordering proof.
 */
struct Cursor *CoordRequestCtx_TakeParkedCursor(CoordRequestCtx *ctx);

/**
 * Store error for reply_callback to handle (pre-request errors).
 * Used when errors occur before AREQ/HREQ is created.
 * If already timed out, just clears the error.
 */
void CoordRequestCtx_ReplyOrStoreError(CoordRequestCtx *ctx, RedisModuleCtx *redisCtx, QueryError *status);

#ifdef __cplusplus
}
#endif
