/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>           // for size_t

#include "hybrid/hybrid_request.h"
#include "rmr/command.h"      // for MRCommand
#include "dist_plan.h"
#include "profile/options.h"  // for ProfileOptions
#include "vector_index.h"     // for VectorQuery
#include "redismodule.h"      // for RedisModuleString, RedisModuleCtx
#include "spec.h"             // for IndexSpec
#include "util/arr/arr.h"     // for arrayof

struct ConcurrentCmdCtx;

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx);

int DistHybridTimeoutFailClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistHybridReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// For testing purposes
// numShards is passed from the main thread to ensure thread-safe access
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            ProfileOptions profileOptions,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp,
                            const VectorQuery *vq,
                            size_t numShards);

#ifdef __cplusplus
}
#endif
