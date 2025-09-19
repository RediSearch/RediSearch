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

#ifdef __cplusplus
extern "C" {
#endif

// Test command for hybrid dispatcher
// Usage: FT.TEST.DISPATCHER <index> <search_query> <vector_field> <vector_value>
int HybridTestDispatcherCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif
