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
#include "hybrid/hybrid_request.h"
#include "asm_state_machine.h"

// TODO ASM: Move keySpaceVersion and innerQueriesCount to BlockedQueryNode, so that we will avoid an extra allocation and deallocation
struct BlockedClientPrivateData {
  void *data;
  uint32_t keySpaceVersion;
  size_t innerQueriesCount;
};

static void FreeQueryNode(RedisModuleCtx* ctx, void *privdata) {
  struct BlockedClientPrivateData *data = (struct BlockedClientPrivateData *)privdata;
  uint32_t keySpaceVersion = data->keySpaceVersion;
  size_t innerQueriesCount = data->innerQueriesCount;
  BlockedQueryNode *queryNode = (BlockedQueryNode *)data->data;
  BlockedQueries_RemoveQuery(queryNode);
  // Function called from the free callbacks to decrease the query count. The ideas is that this callback
  // will be called even in the case of client disconnection, and in any case.
  if (keySpaceVersion != INVALID_KEYSPACE_VERSION) {
    // TODO ASM: Somehow we should know if the query failed or not, and if the query was a cursor query and only decrease the cursor count if it did.
    ASM_AccountRequestFinished(keySpaceVersion, innerQueriesCount);
  }
  rm_free(queryNode);
  rm_free(data);
}

static void FreeCursorNode(RedisModuleCtx* ctx, void *node) {
  BlockedCursorNode *cursorNode = (BlockedCursorNode *)node;
  BlockedQueries_RemoveCursor(cursorNode);
  rm_free(cursorNode);
}

RedisModuleBlockedClient *BlockQueryClient(RedisModuleCtx *ctx, StrongRef spec_ref, AREQ* req, bool hybrid_request) {
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  BlockedQueryNode *node = BlockedQueries_AddQuery(blockedQueries, spec_ref, &req->ast);

  // Prepare context for the worker thread
  // Since we are still in the main thread, and we already validated the
  // spec's existence, it is safe to directly get the strong reference from the spec
  // found in buildRequest.
  struct BlockedClientPrivateData *privdata = rm_malloc(sizeof(struct BlockedClientPrivateData));
  privdata->data = node;
  privdata->keySpaceVersion = req->keySpaceVersion;
  privdata->innerQueriesCount = hybrid_request ? HYBRID_REQUEST_NUM_SUBQUERIES : 1;
  RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, NULL, NULL, FreeQueryNode, 0);
  RedisModule_BlockClientSetPrivateData(blockedClient, privdata);
  // report block client start time
  RedisModule_BlockedClientMeasureTimeStart(blockedClient);
  return blockedClient;
}

RedisModuleBlockedClient *BlockCursorClient(RedisModuleCtx *ctx, Cursor *cursor, size_t count) {
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  RS_LOG_ASSERT(blockedQueries, "MainThread_InitBlockedQueries was not called, or function not called from main thread");
  BlockedCursorNode *node = BlockedQueries_AddCursor(blockedQueries, cursor->spec_ref, cursor->id, &cursor->execState->ast, count);
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
