/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef PARSE_HYBRID_H
#define PARSE_HYBRID_H

#include <stdint.h>
#include <stdbool.h>                   // for bool

#include "redismodule.h"               // for RedisModuleCtx
#include "aggregate/aggregate.h"       // for AREQ, CursorConfig
#include "pipeline/pipeline.h"         // for HybridPipelineParams
#include "search_ctx.h"                // for RedisSearchCtx
#include "hybrid_request.h"
#include "hybrid_scoring.h"
#include "rs_wall_clock.h"             // for rs_wall_clock_ns_t
#include "profile/options.h"           // for ProfileOptions
#include "aggregate/aggregate_plan.h"  // for AGGPlan
#include "config.h"                    // for RequestConfig
#include "query_error.h"               // for QueryError
#include "rmutil/args.h"               // for ArgsCursor

#ifdef __cplusplus
extern "C" {
#endif


#define HYBRID_DEFAULT_KNN_K 10

typedef struct ParseHybridCommandCtx {
    AREQ *search;
    AREQ *vector;
    AGGPlan *tailPlan;
    HybridPipelineParams* hybridParams;
    RequestConfig* reqConfig;
    CursorConfig* cursorConfig;
    rs_wall_clock_ns_t *coordDispatchTime; // Coordinator dispatch time for internal commands
} ParseHybridCommandCtx;

// Function for parsing hybrid command arguments - exposed for testing
int parseHybridCommand(RedisModuleCtx *ctx, ArgsCursor *ac,
                       RedisSearchCtx *sctx, ParseHybridCommandCtx *parsedCmdCtx,
                       QueryError *status, bool internal, ProfileOptions profileOptionsl);

#ifdef __cplusplus
}
#endif

#endif //PARSE_HYBRID_H
