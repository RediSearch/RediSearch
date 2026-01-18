/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/#pragma once

#include "redismodule.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int IndexObfuscatedInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * Output basic index info in a crash-safe manner (no allocations, no locks).
 * Can be called from crash/signal handlers.
 */
void IndexInfoCrashSafe(const IndexSpec *sp, RedisModuleInfoCtx *info_ctx, bool obfuscate);

#ifdef __cplusplus
}
#endif
