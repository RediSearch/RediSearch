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

static QueryRequest *BlockedRequestCtx_QueryRequest(BlockedRequestCtx *brc) {
  return brc->kind == REQUEST_KIND_AREQ ? (QueryRequest *)brc->query.areq
                                       : (QueryRequest *)brc->query.hybrid;
}

void BlockedRequestCtx_BeginCycle(BlockedRequestCtx *brc, RedisModuleBlockedClient *bc,
                                  RedisModuleCmdFunc reply_cb) {
  QueryRequest *request = BlockedRequestCtx_QueryRequest(brc);
  // No overlapping cycles: the previous cycle's OnFree must have run before a
  // new cycle may begin on the same wrapper. This holds structurally for
  // blocked cursor cycles — the cursor is only parked back into the idle list
  // by OnFree (cursorEndOfCycle records the disposition; OnFree executes it),
  // so no other client can take it before the cycle fully ended.
  RS_ASSERT(brc->bc == NULL && request->registryInfo.node == NULL &&
            request->registryInfo.kind == REGISTRY_ENTRY_NONE);
  // The cycle's hold on the wrapper: keeps the wrapper (and the owned request)
  // alive until OnFree, so the reply/timeout callbacks may dereference the
  // privdata even if the BG worker released its own hold (e.g. a cursor freed
  // on ITERDONE) before the client was unblocked.
  // TRANSITIONAL(MOD-16691): expressed through the refcount bridge until the
  // cursor-ownership step makes the cycle the single owner.
  BlockedRequestCtx_IncrRef(brc);
  BlockedRequestCtx_QueryRequest(brc)->blockedClientCycleActive = true;
  // TODO($$$): Remove the legacy cycle marker once consumers use QueryRequest.blockedClientCycleActive.
  brc->bc = bc;
  brc->deferred_reply = (reply_cb != NULL);
  RS_AtomicIntStoreRelaxed(
      &BlockedRequestCtx_QueryRequest(brc)->async.strictReadOwner, BRC_READ_OWNER_NONE);
  // TODO($$$): Remove the legacy async state once consumers use QueryRequest.async.
  atomic_store_explicit(&brc->strictReadOwner, BRC_READ_OWNER_NONE, memory_order_relaxed);
  RedisModule_BlockClientSetPrivateData(bc, brc);
}

void BlockedRequestCtx_EndCycle(BlockedRequestCtx *brc) {
  QueryRequest *request = BlockedRequestCtx_QueryRequest(brc);
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
  // AREQ_CleanUpStoredCursor frees the cursor with its wrapper handle intact,
  // so Cursor_FreeInternal releases the cursor's wrapper reference. Skipping
  // this and letting ChunkReplyState_Destroy handle the stash would leak that
  // reference (it deliberately clears the handle first — correct only in the
  // refcount==0 context of BlockedRequestCtx_Free). Disposing the stash here is
  // also what prevents the RETURN_STRICT preempt path from leaking the cursor
  // reserved by an initial WITHCURSOR query.
  if (brc->kind == REQUEST_KIND_AREQ) {
    AREQ_CleanUpStoredCursor(brc->query.areq);
  }
  // Per-cycle reply-state teardown: dispose whatever the reply callback did
  // not consume (unconsumed stored results when the timeout replied first).
  // Idempotent; also runs in BlockedRequestCtx_Free as a safety net.
  QueryRequest_ResetReply(request);

  // TODO($$$): Remove the legacy reply state once all consumers use QueryRequest.reply.
  ChunkReplyState_Destroy(&brc->reply);
  // TODO($$$): Remove the legacy reply state once all consumers use QueryRequest.reply.
  brc->reply.hasStoredResults = false;
  request->blockedClientCycleActive = false;
  // TODO($$$): Remove the legacy cycle marker once consumers use QueryRequest.blockedClientCycleActive.
  brc->bc = NULL;
  brc->deferred_reply = false;
  request->cursorInfo.cursor = NULL;
  request->cursorInfo.disposition = CURSOR_DISPOSITION_NONE;
  // TODO($$$): Remove the legacy cursor disposition once consumers use QueryRequest.cursorInfo.
  brc->cursor = NULL;
  brc->cursor_dispose_free = false;
}

void BlockedRequestCtx_OnFree(RedisModuleCtx *ctx, void *privdata) {
  BlockedRequestCtx *brc = privdata;
#ifdef ENABLE_ASSERT
  // Debug-only counter so tests can deterministically observe that
  // free_privdata fired without blocking the main thread in the callback.
  BlockedRequestOnFreeDebug_Increment();
#endif
  // Execute the cycle's cursor disposition (snapshot first — EndCycle clears
  // the per-cycle fields). Parking here, after the reply/timeout callback, is
  // what keeps a cycle's cursor unreachable to other clients mid-cycle.
  // Cursor_Pause converts park to free when CURSOR DEL marked the cursor
  // mid-cycle (delete_mark).
  struct Cursor *cursor = brc->cursor;
  bool dispose_free = brc->cursor_dispose_free;
  BlockedRequestCtx_EndCycle(brc);
  if (cursor) {
    if (dispose_free) {
      Cursor_Free(cursor);
    } else {
      Cursor_Pause(cursor);
    }
  }
  BlockedRequestCtx_DecrRef(brc);
}

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      BlockedRequestCtx *brc,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms) {
  // If a timeout is armed, both callbacks must be provided.
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  // Registry bookkeeping only (FT.INFO / crash reports). The callbacks reach
  // the request through the blocked client's privdata (the BlockedRequestCtx),
  // so the node carries no privdata and holds no reference. Unlinked in
  // EndCycle; TRANSITIONAL(MOD-16691): Step 3 links the wrapper itself instead.
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         BlockedRequestCtx_OnFree, timeout_ms);
  BlockedRequestCtx_BeginCycle(brc, bc, reply_cb);
  BlockedRequestCtx_QueryRequest(brc)->registryInfo = (RegistryInfo) {
    .node = node,
    .kind = REGISTRY_ENTRY_QUERY,
  };
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor,
                                                       size_t count,
                                                       BlockedRequestCtx *brc,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms) {
  RS_ASSERT(cursor->query != NULL);
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");

  // Registry bookkeeping only; see BlockQueryClientWithTimeout.
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref,
                                                     cursor->id, count, NULL, NULL);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         BlockedRequestCtx_OnFree, timeout_ms);
  BlockedRequestCtx_BeginCycle(brc, bc, reply_cb);
  // Cursor cycles reuse the wrapper across reads: reset the per-read
  // RETURN_STRICT claim/latch state so the new cycle starts from a clean
  // slate.
  if (brc->requiresAggregateResultsSync) {
    AREQ_ResetForCursorReadReturnStrict(BlockedRequestCtx_GetAREQ(brc));
  }
  BlockedRequestCtx_QueryRequest(brc)->registryInfo = (RegistryInfo) {
    .node = node,
    .kind = REGISTRY_ENTRY_CURSOR,
  };
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}
