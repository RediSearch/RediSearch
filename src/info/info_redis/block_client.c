/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdatomic.h>

#include "redismodule.h"
#include "util/references.h"
#include "info/info_redis/types/blocked_queries.h"
#include "threads/main_thread.h"
#include "cursor.h"
#include "hybrid/hybrid_request.h"
#include "info/info_redis/block_client.h"
#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

static void QueryRequest_OnDisconnect(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
  UNUSED(ctx);
  QueryRequest *request = RedisModule_BlockClientGetPrivateData(bc);
  RS_ASSERT(request);

  QueryRequestTimeout_MarkTimedOut(&request->timeout);
  if (request->kind == QUERY_REQUEST_KIND_HYBRID) {
    HybridRequest_PropagateTimeoutToSubqueries(QueryRequest_GetHybrid(request));
  }
}

void QueryRequest_BeginCycle(QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb) {
  // No overlapping cycles: the previous cycle's OnFree must have run before a
  // new cycle may begin on the same request. This holds structurally for
  // blocked cursor cycles — the cursor is only parked back into the idle list
  // at cycle end (AREQ_CursorEndOfCycle records the disposition; EndCycle executes
  // it), so no other client can take it before the cycle fully ended.
  RS_ASSERT(!request->blockedClientCycleActive && request->registryInfo.node == NULL &&
            request->registryInfo.kind == REGISTRY_ENTRY_NONE);
  request->blockedClientCycleActive = true;
  QueryRequest_SetUseReplyCallback(request, reply_cb != NULL);
  RS_AtomicIntStoreRelaxed(&request->async.strictReadOwner, QUERY_REQUEST_READ_OWNER_NONE);
  RedisModule_BlockClientSetPrivateData(bc, request);
  // RETURN uses a worker-owned clock deadline rather than the blocked-client
  // atomic and intentionally retains its existing disconnect behavior.
  if (request->timeout.policy != TimeoutPolicy_Return) {
    RedisModule_SetDisconnectCallback(bc, QueryRequest_OnDisconnect);
  }
}

void QueryRequest_EndCycle(QueryRequest *request) {
  if (request->registryInfo.node) {
    if (request->registryInfo.kind == REGISTRY_ENTRY_CURSOR) {
      BlockedQueries_RemoveCursor(request->registryInfo.node);
    } else {
      RS_ASSERT(request->registryInfo.kind == REGISTRY_ENTRY_QUERY);
      BlockedQueries_RemoveQuery(request->registryInfo.node);
    }
    rm_free(request->registryInfo.node);
  }
  request->registryInfo.node = NULL;
  request->registryInfo.kind = REGISTRY_ENTRY_NONE;
  // Per-cycle reply-state teardown: dispose whatever the reply callback did
  // not consume (unconsumed stored results when the timeout replied first).
  // Idempotent; base destruction also clears reply state as a safety net.
  QueryRequest_ResetReply(request);

  // Snapshot the disposition before clearing the per-cycle fields it lives in.
  struct Cursor *cursor = request->cursorInfo.cursor;
  CursorDisposition disposition = request->cursorInfo.disposition;
  request->blockedClientCycleActive = false;
  request->cursorInfo.cursor = NULL;
  request->cursorInfo.disposition = CURSOR_DISPOSITION_FREE;

  // Parking only here, at cycle end, is what keeps the cycle's cursor
  // unreachable to other clients mid-cycle. Cursor_Pause converts park to
  // free when CURSOR DEL marked the cursor mid-cycle (delete_mark).
  if (cursor) {
    if (disposition == CURSOR_DISPOSITION_PAUSE) {
      Cursor_Pause(cursor);
    } else {
      Cursor_Free(cursor);
    }
  } else {
    QueryRequest_Free(request);
  }
}

void QueryRequest_OnFree(RedisModuleCtx *ctx, void *privdata) {
  QueryRequest *request = privdata;
#ifdef ENABLE_ASSERT
  // Debug-only counter so tests can deterministically observe that
  // free_privdata fired without blocking the main thread in the callback.
  QueryRequestOnFreeDebug_Increment();
#endif
  QueryRequest_EndCycle(request);
}

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      QueryRequest *request,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms) {
  // If a timeout is armed, both callbacks must be provided.
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  // Registry bookkeeping only (FT.INFO / crash reports). The callbacks reach
  // the request through the blocked client's private data,
  // so the node carries no privdata and holds no reference. Unlinked in
  // EndCycle; TRANSITIONAL(MOD-16691): link the request itself instead.
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         QueryRequest_OnFree, timeout_ms);
  QueryRequest_BeginCycle(request, bc, reply_cb);
  request->registryInfo = (RegistryInfo) {
    .node = node,
    .kind = REGISTRY_ENTRY_QUERY,
  };
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor,
                                                       size_t count,
                                                       QueryRequest *request,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms) {
  RS_ASSERT(cursor->query == request);
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");

  // Registry bookkeeping only; see BlockQueryClientWithTimeout.
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref,
                                                     cursor->id, count, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         QueryRequest_OnFree, timeout_ms);
  QueryRequest_BeginCycle(request, bc, reply_cb);
  // Publish the cycle's cursor handle up front, on the main thread. The
  // disposition keeps its FREE default unless the cycle's reply exposes a
  // live cursor id and records PAUSE.
  request->cursorInfo.cursor = cursor;
  // Cursor cycles reuse the request across reads: reset the per-read
  // RETURN_STRICT claim/latch state so the new cycle starts from a clean
  // slate.
  if (request->async.requiresAggregateResultsSync) {
    AREQ_ResetForCursorReadReturnStrict(QueryRequest_GetAREQ(request));
  }
  request->registryInfo = (RegistryInfo) {
    .node = node,
    .kind = REGISTRY_ENTRY_CURSOR,
  };
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}
