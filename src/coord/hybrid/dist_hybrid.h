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
#include "profile/options.h"
#include "vector_index.h"

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx);

// For testing purposes
// If outKArgIndex is not NULL, stores the index of the K value argument in the MRCommand
// (for later modification by the command modifier callback in SHARD_K_RATIO optimization).
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            ProfileOptions profileOptions,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp, int *outKArgIndex);

#ifdef __cplusplus
}
#endif
