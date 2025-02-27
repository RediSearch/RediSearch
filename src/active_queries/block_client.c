/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#include "redismodule.h"
#include "spec.h"
#include "util/references.h"
#include "query.h"
#include "aggregate/aggregate.h"
#include "active_queries/thread_info.h"
#include "active_queries.h"

static void FreeQueryNode(RedisModuleCtx* ctx, void *node) {
  ActiveQueryNode *activeQueryNode = node;
  ActiveQueries_RemoveQuery(activeQueryNode);
  rm_free(activeQueryNode);
}

static void FreeCursorNode(RedisModuleCtx* ctx, void *node) {
  ActiveCursorNode *activeCursorNode = node;
  ActiveQueries_RemoveCursor(activeCursorNode);
  rm_free(activeCursorNode);
}

RedisModuleBlockedClient *BlockQueryClient(RedisModuleCtx *ctx, StrongRef spec_ref, AREQ* req, int timeoutMS) {
  ActiveQueries *activeQueries = GetActiveQueries();
  RS_LOG_ASSERT(activeQueries, "ThreadLocalStorage_Init was not called, or function not called from main thread");
  pthread_t tid = pthread_self();
  ActiveQueryNode *node = ActiveQueries_AddQuery(activeQueries, spec_ref, &req->ast);

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

RedisModuleBlockedClient *BlockCursorClient(RedisModuleCtx *ctx, uint64_t cursorId, size_t count, int timeoutMS) {
    ActiveQueries *activeQueries = GetActiveQueries();
    RS_LOG_ASSERT(activeQueries, "ThreadLocalStorage_Init was not called, or function not called from main thread");
    ActiveCursorNode *node = ActiveQueries_AddCursor(activeQueries, cursorId, count);

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