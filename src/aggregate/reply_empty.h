/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redismodule.h"

int coord_search_query_reply_empty(RedisModuleCtx *ctx);

int coord_aggregate_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int common_hybrid_query_reply_empty(RedisModuleCtx *ctx);

int single_shard_common_query_reply_empty(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
