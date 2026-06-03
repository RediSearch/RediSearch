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

#include <stdbool.h>

#include "redismodule.h"
#include "query_error.h"

// Coordinator empty reply for FT.SEARCH commands.
// Handles both RESP2 and RESP3 with proper search result formatting.
int coord_search_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode);

// Coordinator empty reply for FT.AGGREGATE commands.
// Handles both RESP2 and RESP3 with proper aggregate result formatting.
// Requires command arguments to extract formatting requirements.
int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryErrorCode errCode);

// Empty reply for hybrid queries. Currently used during OOM conditions and pre-execution timeouts.
// Creates QueryError with OOM/timeout warning and uses sendChunk_ReplyOnly_HybridEmptyResults.
// When isProfile is true, wraps the reply with profile structure.
int common_hybrid_query_reply_empty(RedisModuleCtx *ctx, QueryErrorCode errCode, bool internal, bool isProfile);

// Single-shard empty reply for SEARCH and AGGREGATE commands.
// Handles both RESP2 and RESP3 with command-appropriate formatting.
// Works for both SEARCH and AGGREGATE by compiling query for format detection.
int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int execOptions, QueryErrorCode errCode);

// Empty cursor-shaped reply for FT.CURSOR READ on the RETURN_STRICT timeout
// fast-path. Preserves `cid` so the client can retry, and bumps the timeout
// warning stats counter for parity with `AREQ_ReplyWithStoredResults`.
// Use `internal=true` for shard (_FT.CURSOR READ) calls, `false` for coord.
// The thin wrappers below are kept for call sites that lack a live AREQ.
int cursor_read_empty_reply_timeout(RedisModuleCtx *ctx, long long cid, bool internal);
int coord_cursor_read_empty_reply_timeout(RedisModuleCtx *ctx, long long cid);
int shard_cursor_read_empty_reply_timeout(RedisModuleCtx *ctx, long long cid);
