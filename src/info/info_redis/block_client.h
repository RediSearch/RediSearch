/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include "redismodule.h"
#include "config.h"
#include "util/references.h"
#include "rs_wall_clock.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;
struct Cursor;
struct BlockedRequestCtx;

/* Blocked-client cycle API for BlockedRequestCtx (see aggregate.h).
 *
 * A cycle is one blocked-client round trip: the initial query execution or a
 * single cursor read. The wrapper is the blocked client's privdata for the
 * whole cycle; BlockedRequestCtx_OnFree is the free_privdata callback and the
 * single main-thread teardown point. The canonical sequence at every
 * query-shaped call site is:
 *
 *   bc = BlockQueryClientWithTimeout(ctx, spec_ref, brc, reply_cb, timeout_cb,
 *                                    timeout_ms);
 *   <dispatch to worker pool>;
 *
 * (BlockCursorClientWithTimeout for cursor reads.) Both helpers call
 * RedisModule_BlockClient with OnFree registered, then
 * BlockedRequestCtx_BeginCycle to bind the per-cycle fields. */

/* Bind the per-cycle fields on `brc`. Called on the main thread after
 * RedisModule_BlockClient returned `bc` (with BlockedRequestCtx_OnFree
 * registered as free_privdata) and before dispatching BG work. Takes the
 * cycle's reference on the wrapper (TRANSITIONAL(MOD-16691) refcount bridge
 * until the cursor-ownership step), sets `brc` as the blocked client's
 * privdata, records the cycle's reply mode (`reply_cb` must be the value that
 * was passed to RedisModule_BlockClient), and performs the per-read
 * RETURN_STRICT reset for AREQ cursor cycles. */
void BlockedRequestCtx_BeginCycle(struct BlockedRequestCtx *brc, RedisModuleBlockedClient *bc,
                                  RedisModuleCmdFunc reply_cb);

/* Unlink the cycle's registry node and clear the per-cycle fields. Called from
 * OnFree; callable directly only in tests. */
void BlockedRequestCtx_EndCycle(struct BlockedRequestCtx *brc);

/* The free_privdata callback registered with RedisModule_BlockClient. Runs on
 * the main thread after the reply or timeout callback, before the blocked
 * client is destroyed. Ends the cycle and releases the cycle's hold on the
 * wrapper. */
void BlockedRequestCtx_OnFree(RedisModuleCtx *ctx, void *privdata);

/* Block `ctx` for one query cycle of `brc`. Registers the cycle in
 * BlockedQueries, calls RedisModule_BlockClient(reply_cb, timeout_cb,
 * BlockedRequestCtx_OnFree, timeout_ms) and BeginCycle. `reply_cb`/`timeout_cb`
 * may both be NULL (inline reply mode) but must be provided together with a
 * non-zero `timeout_ms`. */
RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      struct BlockedRequestCtx *brc,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms);

/* Same as BlockQueryClientWithTimeout for one cursor-read cycle. */
RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, struct Cursor *cursor,
                                                       size_t count,
                                                       struct BlockedRequestCtx *brc,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms);

#ifdef __cplusplus
}
#endif
