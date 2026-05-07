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

static void FreeQueryNode(RedisModuleCtx* ctx, void *node) {
  BlockedQueryNode *queryNode = node;
  // Call the callback to free privdata if provided
  if (queryNode->freePrivData && queryNode->privdata) {
    queryNode->freePrivData(queryNode->privdata);
  }
  BlockedQueries_RemoveQuery(queryNode);
  rm_free(queryNode);
}

static void FreeCursorNode(RedisModuleCtx* ctx, void *node) {
  BlockedCursorNode *cursorNode = node;
  if (cursorNode->freePrivData && cursorNode->privdata) {
    cursorNode->freePrivData(cursorNode->privdata);
  }
  BlockedQueries_RemoveCursor(cursorNode);
  rm_free(cursorNode);
}

RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec_ref, BlockClientCtx *blockClientCtx) {
  // Assert that if timeoutMS is provided, then both callbacks must be provided.
  RS_ASSERT(blockClientCtx->timeoutMS == 0 || (blockClientCtx->timeoutCallback != NULL && blockClientCtx->replyCallback != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  // privdata ownership: shared between blockedClientReqCtx (background thread) and BlockedQueryNode (timeout callback, reply callback).
  // Take a reference for the timeout callback access via node->privdata.
  // This reference is released in FreeQueryNode via the freePrivData callback after timeout/reply callback completes.
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, blockClientCtx->ast, blockClientCtx->privdata,
                                                    blockClientCtx->freePrivData);

  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, blockClientCtx->replyCallback, blockClientCtx->timeoutCallback, FreeQueryNode, blockClientCtx->timeoutMS);
  RedisModule_BlockClientSetPrivateData(blockedClient, node);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}

RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor, size_t count, BlockClientCtx *blockClientCtx) {
  RS_ASSERT(blockClientCtx != NULL);
  RS_ASSERT(cursor->execState != NULL);
  RS_ASSERT(blockClientCtx->timeoutMS == 0 ||
            (blockClientCtx->timeoutCallback != NULL && blockClientCtx->replyCallback != NULL));

  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");

  // privdata is shared between the worker and BlockedCursorNode (timeout/reply
  // callbacks). Caller takes the extra ref (e.g. AREQ_IncrRef on FAIL);
  // FreeCursorNode releases it via freePrivData.
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref, cursor->id,
                                                     &cursor->execState->ast, count,
                                                     blockClientCtx->privdata,
                                                     blockClientCtx->freePrivData);

  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, blockClientCtx->replyCallback,
      blockClientCtx->timeoutCallback, FreeCursorNode, blockClientCtx->timeoutMS);
  RedisModule_BlockClientSetPrivateData(blockedClient, node);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}
