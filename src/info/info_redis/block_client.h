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
 *   bc = BlockQueryClientWithTimeout(ctx, request, reply_cb, timeout_cb,
 *                                    timeout_ms);
 *   <dispatch to worker pool>;
 *
 * (BlockCursorClientWithTimeout for cursor reads.) Both helpers call
 * RedisModule_BlockClient with OnFree registered, bind the per-cycle fields,
 * and register the standalone cycle for crash reporting. */

/* Bind the per-cycle fields on `request`. Called on the main thread after
 * RedisModule_BlockClient returned `bc` (with QueryRequest_OnFree
 * registered as free_privdata) and before dispatching BG work. Takes
 * ownership of the request (it becomes the blocked client's privdata — see
 * the ownership contract on QueryRequest) and records the cycle's reply mode
 * (`reply_cb` must be the value that was passed to RedisModule_BlockClient).
 * Coordinator call sites use this lifecycle API without registering in
 * BlockedQueries. */
void QueryRequest_BeginCycle(struct QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb);

/* Same lifecycle setup as QueryRequest_BeginCycle for a cursor read. */
void QueryRequest_BeginCursorCycle(struct QueryRequest *request, RedisModuleBlockedClient *bc,
                                   RedisModuleCmdFunc reply_cb);

/* End the cycle: unlink the request from the registry, drain unconsumed
 * per-cycle reply state, then dispose of the request — execute the recorded
 * cursor disposition, or free it. Called from OnFree. */
void QueryRequest_EndCycle(struct QueryRequest *request);

/* The free_privdata callback registered with RedisModule_BlockClient. Runs on
 * the main thread after the reply or timeout callback, before the blocked
 * client is destroyed; delegates to QueryRequest_EndCycle. */
void QueryRequest_OnFree(RedisModuleCtx *ctx, void *privdata);

/* Shutdown-only: end every standalone cycle still linked in the registry,
 * leaving it empty (BlockedQueries_Free asserts that). RedisModule_UnblockClient
 * only queues an unblock, and cleanup runs synchronously inside the SHUTDOWN
 * event, so callbacks queued while the workers were draining never run. Call
 * only after the workers pool has stopped; coordinator cycles do not register
 * here. */
void BlockedQueries_UnwindCycles(void);

/* Block `ctx` for one query cycle of `request`. Registers the cycle in
 * BlockedQueries, calls RedisModule_BlockClient(reply_cb, timeout_cb,
 * QueryRequest_OnFree, timeout_ms) and BeginCycle. `reply_cb`/`timeout_cb`
 * may both be NULL (inline reply mode) but must be provided together with a
 * non-zero `timeout_ms`. */
RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx,
                                                      struct QueryRequest *request,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms);

/* Same as BlockQueryClientWithTimeout for one cursor-read cycle; also resets
 * the per-read RETURN_STRICT claim/latch state (the request is reused across
 * reads). */
RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, struct Cursor *cursor,
                                                       struct QueryRequest *request,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms);

#ifdef __cplusplus
}
#endif
