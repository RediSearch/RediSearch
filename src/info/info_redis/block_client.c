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
#include "config.h"
#include "obfuscation/obfuscation_api.h"
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
  // by OnFree (cursorEndOfCycle records the disposition; OnFree executes it),
  // so no other client can take it before the cycle fully ended.
  RS_ASSERT(!request->blockedClientCycleActive && !RegistryInfo_IsLinked(&request->registryInfo));
  // The cycle's hold keeps the request alive until OnFree, so the reply/timeout
  // callbacks may dereference the privdata even if the BG worker released its
  // own hold (e.g. a cursor freed on ITERDONE) before the client was unblocked.
  QueryRequest_IncrRef(request);
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

const char *QueryRequest_ReportIndexName(const QueryRequest *request, char *obfuscated_buffer) {
  if (request->args.argc < 2) {
    return "n/a";
  }
  // The request's held argv mirrors the logical command — argv[0] is the
  // command, argv[1] the index as the caller addressed it (an alias included).
  // Plain reads and pure hashing only: this also runs in the crash handler's
  // signal context.
  size_t len;
  const char *name = RedisModule_StringPtrLen(request->args.argv[1], &len);
  if (!RSGlobalConfig.hideUserDataFromLog) {
    return name;
  }
  // Same derivation as the spec's own obfuscated name (sha1 of the name), so
  // crash entries correlate with the rest of the log unless addressed by
  // alias.
  Sha1 sha1;
  Sha1_Compute(name, len, &sha1);
  Obfuscate_Index(&sha1, obfuscated_buffer);
  return obfuscated_buffer;
}
