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
struct QueryRequest;

/* Blocked-client cycle API for QueryRequest (see query_request.h).
 *
 * A cycle is one blocked-client round trip: the initial query execution or a
 * single cursor read. The request is the blocked client's private data for the
 * whole cycle; QueryRequest_OnFree is the free_privdata callback and the
 * single main-thread teardown point. The canonical sequence at every
 * query-shaped call site is:
 *
 *   bc = BlockQueryClientWithTimeout(ctx, spec_ref, request, reply_cb, timeout_cb,
 *                                    timeout_ms);
 *   <dispatch to worker pool>;
 *
 * (BlockCursorClientWithTimeout for cursor reads.) Both helpers call
 * RedisModule_BlockClient with OnFree registered, then
 * QueryRequest_BeginCycle to bind the per-cycle fields. */

/* Bind the per-cycle fields on `request`. Called on the main thread after
 * RedisModule_BlockClient returned `bc` (with QueryRequest_OnFree
 * registered as free_privdata) and before dispatching BG work. Takes the
 * cycle's reference on the request, sets `request` as the blocked client's
 * privdata, and records the cycle's reply mode (`reply_cb` must be the value
 * that was passed to RedisModule_BlockClient). */
void QueryRequest_BeginCycle(struct QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb);

/* Unlink the cycle's registry node and clear the per-cycle fields. Called from
 * OnFree; callable directly only in tests. */
void QueryRequest_EndCycle(struct QueryRequest *request);

/* The free_privdata callback registered with RedisModule_BlockClient. Runs on
 * the main thread after the reply or timeout callback, before the blocked
 * client is destroyed. Ends the cycle and releases the cycle's request hold. */
void QueryRequest_OnFree(RedisModuleCtx *ctx, void *privdata);

/* Block `ctx` for one query cycle of `request`. Registers the cycle in
 * BlockedQueries, calls RedisModule_BlockClient(reply_cb, timeout_cb,
 * QueryRequest_OnFree, timeout_ms) and BeginCycle. `reply_cb`/`timeout_cb`
 * may both be NULL (inline reply mode) but must be provided together with a
 * non-zero `timeout_ms`. */
RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      struct QueryRequest *request,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms);

/* Same as BlockQueryClientWithTimeout for one cursor-read cycle; also resets
 * the per-read RETURN_STRICT claim/latch state (the request is reused across
 * reads). */
RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, struct Cursor *cursor,
                                                       size_t count,
                                                       struct QueryRequest *request,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms);

#ifdef __cplusplus
}
#endif
