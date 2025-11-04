/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Empty Reply Module - functions early bailout and return empty results instead of failing queries.
// Handles different query types and contexts with proper protocol formatting.

#pragma once

#include "redismodule.h"
#include "query_error.h"

// Coordinator empty reply for FT.SEARCH commands.
// Handles both RESP2 and RESP3 with proper search result formatting.
int coord_search_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode);

// Coordinator empty reply for FT.AGGREGATE commands.
// Handles both RESP2 and RESP3 with proper aggregate result formatting.
// Requires command arguments to extract formatting requirements.
int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode);

// Empty reply for hybrid queries.
// Uses RESP3 map structure with proper hybrid result formatting.
// Works for both coordinator and single-shard hybrid queries.
int common_hybrid_query_reply_empty(RedisModuleCtx *ctx, QueryErrorCode errCode);

// Single-shard empty reply for SEARCH and AGGREGATE commands.
// Handles both RESP2 and RESP3 with command-appropriate formatting.
// Works for both SEARCH and AGGREGATE by compiling query for format detection.
int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int execOptions, QueryErrorCode errCode);
