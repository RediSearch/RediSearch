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
#include "hybrid/hybrid_scoring.h"

// Resolved COMBINE parameters captured on the coordinator, used to reconstruct
// an old-shard-compatible COMBINE clause on the wire (see HybridRequest_buildMRCommand).
typedef struct {
  // Resolved scoring parameters. Borrowed, not owned: points at the request's
  // HybridScoringContext, which the merger keeps alive past command building.
  const HybridScoringContext *scoringCtx;
  // YIELD_SCORE_AS alias for the combined score, or NULL. Carried alongside
  // scoringCtx because HybridScoringContext has no alias field.
  const char *scoreAlias;
} HybridCombineWireParams;

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx);

int DistHybridTimeoutFailClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistHybridReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// For testing purposes
// numShards is passed from the main thread to ensure thread-safe access
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            ProfileOptions profileOptions,
                            const HybridCombineWireParams *combineParams,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp,
                            const VectorQuery *vq,
                            size_t numShards);

#ifdef __cplusplus
}
#endif
