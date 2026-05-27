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

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref,
                                                      RequestSyncCtx *rsc, QueryAST *ast,
                                                      BlockedClientReplyCB replyCallback,
                                                      BlockedClientTimeoutCB timeoutCallback,
                                                      rs_wall_clock_ms_t timeoutMS) {
  UNUSED(spec_ref);
  UNUSED(ast);
  // Assert that if timeoutMS is provided, then both callbacks must be provided.
  RS_ASSERT(timeoutMS == 0 || (timeoutCallback != NULL && replyCallback != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, replyCallback, timeoutCallback, RequestSyncCtx_OnFree, timeoutMS);
  RedisModule_BlockClientSetPrivateData(blockedClient, rsc);
  RSC_BeginCycle(rsc, replyCallback ? REQUEST_REPLY_DEFERRED : REQUEST_REPLY_INLINE,
                 REQUEST_CYCLE_QUERY, 0, 0);
  BlockedQueries_LinkQuery(blockedQueries, rsc);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor, size_t count,
                                                       RequestSyncCtx *rsc,
                                                       BlockedClientReplyCB replyCallback,
                                                       BlockedClientTimeoutCB timeoutCallback,
                                                       rs_wall_clock_ms_t timeoutMS) {
  AREQ *req = Cursor_GetAREQ(cursor);
  RS_ASSERT(req != NULL);
  RS_ASSERT(timeoutMS == 0 || (timeoutCallback != NULL && replyCallback != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");

  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, replyCallback, timeoutCallback,
      RequestSyncCtx_OnFree, timeoutMS);
  RedisModule_BlockClientSetPrivateData(blockedClient, rsc);
  RSC_BeginCycle(rsc, replyCallback ? REQUEST_REPLY_DEFERRED : REQUEST_REPLY_INLINE,
                 REQUEST_CYCLE_CURSOR, cursor->id, count);
  BlockedQueries_LinkCursor(blockedQueries, rsc);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}
