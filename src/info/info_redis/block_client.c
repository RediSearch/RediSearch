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
#include "info/info_redis/types/blocked_queries.h"
#include "threads/main_thread.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"
#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

// The registry list every main-thread cycle links into. Asserts main-thread
// use: the registry is single-threaded by design.
static BlockedQueries *getBlockedQueries(void) {
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  return blockedQueries;
}

static void beginCycleCommon(QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb, DLLIST *list) {
  // No overlapping cycles: the previous cycle's OnFree must have run before a
  // new cycle may begin on the same request. This holds structurally for
  // blocked cursor cycles — the cursor is only parked back into the idle list
  // at cycle end (AREQ_CursorEndOfCycle records the disposition; EndCycle
  // executes it), so no other client can take it before the cycle fully ended.
  RS_ASSERT(!request->blockedClientCycleActive && !RegistryInfo_IsLinked(&request->registryInfo));
  request->blockedClientCycleActive = true;
  QueryRequest_SetUseReplyCallback(request, reply_cb != NULL);
  RS_AtomicIntStoreRelaxed(&request->async.strictReadOwner, QUERY_REQUEST_READ_OWNER_NONE);
  request->registryInfo.cycle_start = time(NULL);
  dllist_prepend(list, &request->registryInfo.node);
  RedisModule_BlockClientSetPrivateData(bc, request);
}

void QueryRequest_BeginCycle(QueryRequest *request, RedisModuleBlockedClient *bc,
                             RedisModuleCmdFunc reply_cb) {
  beginCycleCommon(request, bc, reply_cb, &getBlockedQueries()->queries);
}

void QueryRequest_BeginCursorCycle(QueryRequest *request, RedisModuleBlockedClient *bc,
                                   RedisModuleCmdFunc reply_cb) {
  beginCycleCommon(request, bc, reply_cb, &getBlockedQueries()->cursors);
}

void QueryRequest_EndCycle(QueryRequest *request) {
  if (RegistryInfo_IsLinked(&request->registryInfo)) {
    dllist_delete(&request->registryInfo.node);
    request->registryInfo.cycle_start = 0;
  }
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

void BlockedQueries_UnwindCycles(void) {
  BlockedQueries *blockedQueries = getBlockedQueries();
  DLLIST *lists[] = {&blockedQueries->queries, &blockedQueries->cursors};
  for (size_t i = 0; i < sizeof(lists) / sizeof(*lists); i++) {
    while (!DLLIST_IS_EMPTY(lists[i])) {
      QueryRequest *at = DLLIST_ITEM(lists[i]->next, QueryRequest, registryInfo.node);
      // Unlink only — do NOT run the cycle end. Stopping the pools proves no
      // new cycles start, not that a linked request has no borrowers: a
      // deferred coordinator request (e.g. WITHCOUNT) is still referenced by
      // its MR iterator context until the MR runtimes are gone, so freeing it
      // here would be use-after-free on the IO threads. Leaking it instead is
      // exactly what its queued, never-drained free-privdata callback would
      // have left behind.
      dllist_delete(&at->registryInfo.node);
      at->registryInfo.cycle_start = 0;
    }
  }
}

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx,
                                                      QueryRequest *request,
                                                      RedisModuleCmdFunc reply_cb,
                                                      RedisModuleCmdFunc timeout_cb,
                                                      rs_wall_clock_ms_t timeout_ms) {
  // If a timeout is armed, both callbacks must be provided.
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         QueryRequest_OnFree, timeout_ms);
  QueryRequest_BeginCycle(request, bc, reply_cb);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor,
                                                       QueryRequest *request,
                                                       RedisModuleCmdFunc reply_cb,
                                                       RedisModuleCmdFunc timeout_cb,
                                                       rs_wall_clock_ms_t timeout_ms) {
  RS_ASSERT(cursor->query == request);
  RS_ASSERT(timeout_ms == 0 || (timeout_cb != NULL && reply_cb != NULL));

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, timeout_cb,
                                                         QueryRequest_OnFree, timeout_ms);
  QueryRequest_BeginCursorCycle(request, bc, reply_cb);
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
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}
