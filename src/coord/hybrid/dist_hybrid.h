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
void DEBUG_RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                            struct ConcurrentCmdCtx *cmdCtx);

int DistHybridTimeoutFailClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistHybridReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

struct dict;  // forward decl (see util/dict/dict.h): the resolved PARAMS dict

// Coordinator-resolved state that HybridRequest_buildMRCommand forwards to the
// shards.
typedef struct {
  ProfileOptions profileOptions;
  HybridCombineWireParams combine;
  // Resolved parameter dictionary, or NULL when the client supplied no PARAMS.
  struct dict *params;
  // TIMEOUT forwarding: when forwardTimeout is true, TIMEOUT <timeoutMS> is
  // appended.
  bool forwardTimeout;
  long long timeoutMS;
} HybridShardWireParams;

// Builds the per-shard MR command from the coordinator's parsed hybrid request.
// The function transforms
//   FT.HYBRID index SEARCH query VSIM field vector
// into
//   _FT.HYBRID index SEARCH query VSIM field vector WITHCURSOR _NUM_SSTRING
//   _INDEX_PREFIXES ...
//
// argv/argc are the raw client command; `shardWireParams` (required, non-NULL) carries the
// parsed state to forward. DIALECT is never forwarded (FT.HYBRID rejects it at
// parse time).
//
// Exposed for testing.
// numShards is passed from the main thread to ensure thread-safe access
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            const HybridShardWireParams *shardWireParams,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp,
                            const VectorQuery *vq,
                            size_t numShards);

#ifdef __cplusplus
}
#endif
