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

#include "redismodule.h"

#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "search_ctx.h"
#include "hybrid_request.h"
#include "hybrid_scoring.h"
#include "profile/options.h"

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
} ParseHybridCommandCtx;

// Function for parsing hybrid command arguments - exposed for testing
int parseHybridCommand(RedisModuleCtx *ctx, ArgsCursor *ac,
                       RedisSearchCtx *sctx, ParseHybridCommandCtx *parsedCmdCtx,
                       QueryError *status, bool interna, ProfileOptions profileOptionsl);

#ifdef __cplusplus
}
#endif

#endif //PARSE_HYBRID_H
