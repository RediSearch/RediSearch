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

#include "hybrid/hybrid_request.h"
#include "rmr/command.h"
#include "dist_plan.h"

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx);

// For testing purposes
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp, HybridPipelineParams *hybridParams);

#ifdef __cplusplus
}
#endif
