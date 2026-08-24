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
 * RedisModule_BlockClient with OnFree registered, then
 * QueryRequest_BeginCycle to bind the per-cycle fields. */

/* Bind the per-cycle fields on `request`. Called on the main thread after
 * RedisModule_BlockClient returned `bc` (with QueryRequest_OnFree
 * registered as free_privdata) and before dispatching BG work. Takes
 * ownership of the request (it becomes the blocked client's privdata — see
 * the ownership contract on QueryRequest), links it into the BlockedQueries
 * query registry (crash reports), and records the cycle's reply mode
 * (`reply_cb` must be the value that was passed to RedisModule_BlockClient). */
void QueryRequest_BeginCycle(struct QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb);

/* Same as QueryRequest_BeginCycle, linking into the cursor registry instead. */
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

/* Shutdown-only: run the pending cycle end (QueryRequest_OnFree) for every
 * request still linked in the registry, leaving it empty.
 * RedisModule_UnblockClient only queues an unblock, and module cleanup runs
 * synchronously inside the SHUTDOWN server event, so the main loop never
 * drains the queued free-privdata callbacks of cycles that completed while
 * the pools were shutting down. The process exits without returning to the
 * event loop, so a queued callback cannot run later against a request unwound
 * here — and MODULE UNLOAD is refused while the module has undrained blocked
 * clients. Call only after every pool whose cycles register here has stopped. */
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
