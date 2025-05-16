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

static void FreeQueryNode(RedisModuleCtx* ctx, void *node) {
  BlockedQueryNode *queryNode = node;
  BlockedQueries_RemoveQuery(queryNode);
  rm_free(queryNode);
}

static void FreeCursorNode(RedisModuleCtx* ctx, void *node) {
  BlockedCursorNode *cursorNode = node;
  BlockedQueries_RemoveCursor(cursorNode);
  rm_free(cursorNode);
}

RedisModuleBlockedClient *BlockQueryClient(RedisModuleCtx *ctx, StrongRef spec_ref, AREQ* req, int timeoutMS) {
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, &req->ast);

  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, NULL, NULL, FreeQueryNode, 0);
  RedisModule_BlockClientSetPrivateData(blockedClient, node);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}

RedisModuleBlockedClient *BlockCursorClient(RedisModuleCtx *ctx, Cursor *cursor, size_t count, int timeoutMS) {
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref, cursor->id, count);
  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, NULL, NULL, FreeCursorNode, 0);
  RedisModule_BlockClientSetPrivateData(blockedClient, node);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}
