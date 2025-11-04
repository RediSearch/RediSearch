/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Empty Reply Module - functions to return empty results instead of failing queries.
// Currently used during OOM conditions to return empty results with proper formatting.
// Handles different query types (SEARCH, AGGREGATE, HYBRID) and contexts (single-shard, coordinator).

#include "../module.h"
#include "../aggregate/aggregate.h"
#include "../hybrid/hybrid_exec.h"

// Helper function for empty replies for aggregate-style queries.
// Compiles the query to get request flags and formatting, then uses sendChunk_ReplyOnly_EmptyResults.
// Works for both single-shard and coordinator aggregate queries.
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

// Coordinator empty reply for FT.SEARCH commands. Currently used during OOM conditions.
// Creates a minimal searchRequestCtx with OOM flag and uses sendSearchResults_EmptyResults.
int coord_search_query_reply_empty(RedisModuleCtx *ctx) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    searchRequestCtx req = {0};
    req.queryOOM = true;
    sendSearchResults_EmptyResults(reply, &req);
    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
}

// Coordinator empty reply for FT.AGGREGATE commands. Currently used during OOM conditions.
// Uses the common helper which compiles the query to get formatting requirements.
int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return empty_sendChunk_common(ctx, argv, argc);
}

// Empty reply for hybrid queries. Currently used during OOM conditions.
// Creates QueryError with OOM warning and uses sendChunk_ReplyOnly_HybridEmptyResults.
int common_hybrid_query_reply_empty(RedisModuleCtx *ctx) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    QueryError qerr = QueryError_Default();
    QueryError_SetQueryOOMWarning(&qerr);
    sendChunk_ReplyOnly_HybridEmptyResults(reply, &qerr);
    return REDISMODULE_OK;
}

// Single-shard empty reply for both SEARCH and AGGREGATE commands. Currently used during OOM conditions.
// Uses the common helper which compiles the query and works for both command types.
int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return empty_sendChunk_common(ctx, argv, argc);
}
