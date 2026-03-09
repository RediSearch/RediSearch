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
#include "rmutil/util.h"
#include "reply_empty.h"
#include "info/global_stats.h"
#include "../profile/options.h"

// Helper function that performs minimal parsing of query arguments to support sendChunk output
static int shallow_parse_query_args(RedisModuleString **argv, int argc, AREQ *req) {
    // Check specifically for CURSOR
    if (RMUtil_ArgIndex("WITHCURSOR", argv, argc) != -1) {
        AREQ_AddRequestFlags(req, QEXEC_F_IS_CURSOR);
    }
    // Parse format
    int formatIndex = RMUtil_ArgExists("FORMAT", argv, argc, 1);
    if (formatIndex > 0) {
        formatIndex++;
        ArgsCursor ac;
        ArgsCursor_InitRString(&ac, argv+formatIndex, argc-formatIndex);
        if (parseValueFormat(&req->reqflags, &ac, AREQ_QueryProcessingCtx(req)->err) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}


// Helper function for empty replies for aggregate-style queries.
// Compiles the query to get request flags and formatting, then uses sendChunk_ReplyOnly_EmptyResults.
// Works for both single-shard and coordinator aggregate queries.
// Assumes req has already been compiled, including REQFLAGS and AREQ_QueryProcessingCtx(req)->err has been set.
static int empty_sendChunk_common(RedisModuleCtx *ctx, AREQ *req) {

    sendChunk_ReplyOnly_EmptyResults(ctx, req);

    AREQ_DecrRef(req);
    return REDISMODULE_OK;
}

// Coordinator empty reply for FT.SEARCH commands. Currently used during OOM conditions.
// Creates a minimal searchRequestCtx with OOM flag and uses sendSearchResults_EmptyResults.
int coord_search_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode) {
    searchRequestCtx req = {0};

    // The clock is not important for the empty reply, but is required for profiling
    rs_wall_clock_init(&req.initClock);

    // PROFILE for FT.SEARCH requires no additional parsing
    QueryError status = QueryError_Default();
    if (rscParseProfile(&req, argv) != REDISMODULE_OK) {
        return QueryError_ReplyAndClear(ctx, &status);
    }

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

    // Handle known errors supported by empty reply module
    req.queryOOM = errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY;
    req.timedOut = errCode == QUERY_ERROR_CODE_TIMED_OUT;

    sendSearchResults_EmptyResults(reply, &req);

    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
}

// Coordinator empty reply for FT.AGGREGATE commands. Currently used during OOM conditions.
// Uses the common helper which compiles the query to get formatting requirements.
int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode) {

    AREQ *req = AREQ_New();
    QueryError status = QueryError_Default();
    AREQ_QueryProcessingCtx(req)->err = &status;

    int profileArgs = parseProfileArgs(argv, argc, req);
    if (profileArgs == -1) return RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));

    if (shallow_parse_query_args(argv + profileArgs, argc - profileArgs, req) != REDISMODULE_OK) {
        AREQ_DecrRef(req);
        return QueryError_ReplyAndClear(ctx, &status);
    }

    // Set the error code after compiling the query, since we don't want to overwrite
    // any errors that might have occurred during compilation
    QueryError_SetError(&status, errCode, NULL);
    QueryError_SetCode(&status, errCode);
    if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
        QueryError_SetQueryOOMWarning(&status);
    }

    int ret = empty_sendChunk_common(ctx, req);
    QueryError_ClearError(&status);
    return ret;
}

// Empty reply for hybrid queries. Currently used during OOM conditions.
// Creates QueryError with OOM warning and uses sendChunk_ReplyOnly_HybridEmptyResults.
int common_hybrid_query_reply_empty(RedisModuleCtx *ctx, QueryErrorCode errCode, bool internal) {

    QueryError status = QueryError_Default();
    QueryError_SetError(&status, errCode, NULL);
    QueryError_SetCode(&status, errCode);
    if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
        QueryError_SetQueryOOMWarning(&status);
    }

    // If internal - reply cursor information from shards to coord.
    // Shards notify error by setting cursor id to 0
    if (internal) {
        RedisModule_Reply _coordInfoReply = RedisModule_NewReply(ctx), *coordInfoReply = &_coordInfoReply;
        RedisModule_Reply_Map(coordInfoReply); // root {}
        RedisModule_ReplyKV_LongLong(coordInfoReply, "SEARCH", 0);
        RedisModule_ReplyKV_LongLong(coordInfoReply, "VSIM", 0);
        RedisModule_ReplyKV_Array(coordInfoReply,"warnings"); // warnings []
        if (QueryError_HasQueryOOMWarning(&status)) {
            QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD, 1, SHARD_ERR_WARN);
            RedisModule_Reply_SimpleString(coordInfoReply, QueryError_Strerror(QUERY_ERROR_CODE_OUT_OF_MEMORY));
        }
        RedisModule_Reply_ArrayEnd(coordInfoReply); // ~warnings
        RedisModule_Reply_MapEnd(coordInfoReply); // ~root
        RedisModule_EndReply(coordInfoReply);
        QueryError_ClearError(&status);
        return REDISMODULE_OK;
    }

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    sendChunk_ReplyOnly_HybridEmptyResults(reply, &status);
    RedisModule_EndReply(reply);
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
}

// Single-shard empty reply for both SEARCH and AGGREGATE commands. Currently used during OOM conditions.
// Uses the common helper which compiles the query and works for both command types.
int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int execOptions, QueryErrorCode errCode) {

    AREQ *req = AREQ_New();
    // Clock init required for profiling
    rs_wall_clock_init(&req->profileClocks.initClock);
    rs_wall_clock_init(&AREQ_QueryProcessingCtx(req)->initTime);

    // Check if command in internal
    if (RedisModule_StringPtrLen(argv[0], NULL)[0] == '_') {
        AREQ_AddRequestFlags(req, QEXEC_F_INTERNAL);
    }

    QueryError status = QueryError_Default();
    AREQ_QueryProcessingCtx(req)->err = &status;

    ApplyProfileOptions(AREQ_QueryProcessingCtx(req), &req->reqflags, execOptions);

    if (shallow_parse_query_args(argv, argc, req) != REDISMODULE_OK) {
        AREQ_DecrRef(req);
        return QueryError_ReplyAndClear(ctx, &status);
    }

    // Set the error code after compiling the query, since we don't want to overwrite
    // any errors that might have occurred during compilation
    QueryError_SetError(&status, errCode, NULL);
    QueryError_SetCode(&status, errCode);
    if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
        QueryError_SetQueryOOMWarning(&status);
    }

    int ret = empty_sendChunk_common(ctx, req);
    QueryError_ClearError(&status);
    return ret;
}
