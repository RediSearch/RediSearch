/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "../module.h"
#include "../aggregate/aggregate.h"
#include "../hybrid/hybrid_exec.h"

// Prepare the reply for an empty query for single shard search and single shard/coord aggregate
static int empty_sendChunk_common(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    AREQ *req = AREQ_New();
    QueryError status = QueryError_Default();
    int rc = AREQ_Compile(req, argv + 2, argc - 2, &status);
    if (rc != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
    }
    QueryError_SetQueryOOMWarning(&status);
    sendChunk_ReplyOnly_EmptyResults(reply, AREQ_RequestFlags(req), &status);
    AREQ_Free(req);
    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
}

int coord_search_query_reply_empty(RedisModuleCtx *ctx) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    searchRequestCtx req = {0};
    req.queryOOM = true;
    sendSearchResults_EmptyResults(reply, &req);
    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
}

int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return empty_sendChunk_common(ctx, argv, argc);
}

int common_hybrid_query_reply_empty(RedisModuleCtx *ctx) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    QueryError qerr = QueryError_Default();
    QueryError_SetQueryOOMWarning(&qerr);
    sendChunk_ReplyOnly_HybridEmptyResults(reply, &qerr);
    return REDISMODULE_OK;
}

int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return empty_sendChunk_common(ctx, argv, argc);
}
