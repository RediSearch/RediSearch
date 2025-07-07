//
// Created by Ofir Yanai on 03/07/2025.
//

#ifndef PARSE_HYBRID_H
#define PARSE_HYBRID_H

#include <stdint.h>

#include "redismodule.h"

#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "search_ctx.h"
#include "hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef struct {
  arrayof(AREQ*) requests;
  size_t nrequests;
  QueryError tailError;
  QueryError *errors;
  RequestConfig reqConfig;
  Pipeline tail;
  HybridScoringContext scoringCtx;
} HybridRequest;


  typedef struct HybridPipelineParams {
    AggregationPipelineParams aggregation;
    bool synchronize_read_locks;
    HybridScoringContext *scoringCtx;
  } HybridPipelineParams;

void HybridRequest_Free(HybridRequest *hybridRequest);

int execHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Function for parsing hybrid request parameters - exposed for testing
HybridRequest* parseHybridRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                  RedisSearchCtx *sctx, HybridPipelineParams *hybridParams,
                                  QueryError *status);

#ifdef __cplusplus
}
#endif

#endif //PARSE_HYBRID_H
