/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redismodule.h"
#include "spec.h"
#include "util/references.h"
#include "query.h"
#include "aggregate/aggregate.h"
#include "info/info_redis/types/blocked_queries.h"
#include "threads/main_thread.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"

void BlockedRequestCtx_BeginCycle(BlockedRequestCtx *brc, RedisModuleBlockedClient *bc,
                                  RedisModuleCmdFunc reply_cb, RSTimeoutPolicy policy,
                                  void *coord_ctx) {
  // The cycle's hold on the wrapper: keeps the wrapper (and the owned request)
  // alive until OnFree, so the reply/timeout callbacks may dereference the
  // privdata even if the BG worker released its own hold (e.g. a cursor freed
  // on ITERDONE) before the client was unblocked. Refcount bridge until Step 2b
  // makes the cycle the single owner.
  BlockedRequestCtx_IncrRef(brc);
  brc->bc = bc;
  brc->reply_cb = reply_cb;
  brc->timeout_policy = policy;
  brc->coord_ctx = coord_ctx;
  RedisModule_BlockClientSetPrivateData(bc, brc);
  // Cursor cycles reuse the wrapper across reads: reset the per-read
  // RETURN_STRICT claim/latch state so the new cycle starts from a clean
  // slate. Harmless on the initial cycle (the state is already clean).
  if (brc->kind == REQUEST_KIND_AREQ && brc->requiresAggregateResultsSync) {
    AREQ_ResetForCursorReadReturnStrict(brc->query.areq);
  }
}

void BlockedRequestCtx_EndCycle(BlockedRequestCtx *brc) {
  if (brc->registry_node) {
    if (brc->registry_node_is_cursor) {
      BlockedQueries_RemoveCursor(brc->registry_node);
    } else {
      BlockedQueries_RemoveQuery(brc->registry_node);
    }
    rm_free(brc->registry_node);
    brc->registry_node = NULL;
  }
  brc->bc = NULL;
  brc->reply_cb = NULL;
  brc->coord_ctx = NULL;
}

void BlockedRequestCtx_OnFree(RedisModuleCtx *ctx, void *privdata) {
  BlockedRequestCtx *brc = privdata;
  BlockedRequestCtx_EndCycle(brc);
  // Drain any cursor parked in the stored reply state before releasing the
  // cycle's hold. Guarded no-op on the happy path (the reply callback already
  // cleared the stash) and for queries that never reserved a cursor; disposing
  // it here is what prevents the RETURN_STRICT preempt path from leaking the
  // cursor reserved by an initial WITHCURSOR query (MOD-8477 / PR #10085).
  if (brc->kind == REQUEST_KIND_AREQ) {
    AREQ_CleanUpStoredCursor(brc->query.areq);
  }
  BlockedRequestCtx_DecrRef(brc);
}

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      BlockedRequestCtx *brc,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      RSTimeoutPolicy policy,
                                                      rs_wall_clock_ms_t timeout_ms) {
  // If a timeout is armed, both callbacks must be provided.
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  // Registry bookkeeping only (FT.INFO / crash reports). The callbacks reach
  // the request through the blocked client's privdata (the BlockedRequestCtx),
  // so the node carries no privdata and holds no reference. Unlinked in
  // EndCycle; Step 3 links the wrapper itself instead.
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         BlockedRequestCtx_OnFree, timeout_ms);
  BlockedRequestCtx_BeginCycle(brc, bc, reply_cb, policy, NULL);
  brc->registry_node = node;
  brc->registry_node_is_cursor = false;
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor,
                                                       size_t count,
                                                       BlockedRequestCtx *brc,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       RSTimeoutPolicy policy,
                                                       rs_wall_clock_ms_t timeout_ms) {
  RS_ASSERT(cursor->execState != NULL);
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");

  // Registry bookkeeping only; see BlockQueryClientWithTimeout.
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref,
                                                     cursor->id, count, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         BlockedRequestCtx_OnFree, timeout_ms);
  BlockedRequestCtx_BeginCycle(brc, bc, reply_cb, policy, NULL);
  brc->registry_node = node;
  brc->registry_node_is_cursor = true;
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}
