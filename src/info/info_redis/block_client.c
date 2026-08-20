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
#include "info/info_redis/block_client.h"
#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

void QueryRequest_BeginCycle(QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb) {
  // No overlapping cycles: the previous cycle's OnFree must have run before a
  // new cycle may begin on the same request. This holds structurally for
  // blocked cursor cycles — the cursor is only parked back into the idle list
  // by OnFree (cursorEndOfCycle records the disposition; OnFree executes it),
  // so no other client can take it before the cycle fully ended.
  RS_ASSERT(!request->blockedClientCycleActive && request->registryInfo.node == NULL &&
            request->registryInfo.kind == REGISTRY_ENTRY_NONE);
  // The cycle's hold keeps the request alive until OnFree, so the reply/timeout
  // callbacks may dereference the privdata even if the BG worker released its
  // own hold (e.g. a cursor freed on ITERDONE) before the client was unblocked.
  QueryRequest_IncrRef(request);
  request->blockedClientCycleActive = true;
  QueryRequest_SetUseReplyCallback(request, reply_cb != NULL);
  RS_AtomicIntStoreRelaxed(&request->async.strictReadOwner, QUERY_REQUEST_READ_OWNER_NONE);
  RedisModule_BlockClientSetPrivateData(bc, request);
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
  // Dispose a stashed cursor reference-releasingly BEFORE the generic destroy:
  // AREQ_CleanUpStoredCursor frees the cursor with its request handle intact,
  // so Cursor_FreeInternal releases the cursor's request reference. Skipping
  // this and letting ChunkReplyState_Destroy handle the stash would leak that
  // reference (it deliberately clears the handle first — correct only in the
  // refcount==0 context of final request destruction). Disposing the stash
  // here also prevents the RETURN_STRICT preempt path from leaking the cursor
  // reserved by an initial WITHCURSOR query.
  if (request->kind == QUERY_REQUEST_KIND_AREQ) {
    AREQ_CleanUpStoredCursor(QueryRequest_GetAREQ(request));
  }
  // Per-cycle reply-state teardown: dispose whatever the reply callback did
  // not consume (unconsumed stored results when the timeout replied first).
  // Idempotent; base destruction also clears reply state as a safety net.
  QueryRequest_ResetReply(request);

  request->blockedClientCycleActive = false;
  request->cursorInfo.cursor = NULL;
  request->cursorInfo.disposition = CURSOR_DISPOSITION_NONE;
}

void QueryRequest_OnFree(RedisModuleCtx *ctx, void *privdata) {
  QueryRequest *request = privdata;
#ifdef ENABLE_ASSERT
  // Debug-only counter so tests can deterministically observe that
  // free_privdata fired without blocking the main thread in the callback.
  QueryRequestOnFreeDebug_Increment();
#endif
  // Execute the cycle's cursor disposition (snapshot first — EndCycle clears
  // the per-cycle fields). Parking here, after the reply/timeout callback, is
  // what keeps a cycle's cursor unreachable to other clients mid-cycle.
  // Cursor_Pause converts park to free when CURSOR DEL marked the cursor
  // mid-cycle (delete_mark).
  struct Cursor *cursor = request->cursorInfo.cursor;
  CursorDisposition disposition = request->cursorInfo.disposition;
  QueryRequest_EndCycle(request);
  if (cursor) {
    if (disposition == CURSOR_DISPOSITION_FREE) {
      Cursor_Free(cursor);
    } else {
      RS_ASSERT(disposition == CURSOR_DISPOSITION_PAUSE);
      Cursor_Pause(cursor);
    }
  }
  QueryRequest_DecrRef(request);
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
