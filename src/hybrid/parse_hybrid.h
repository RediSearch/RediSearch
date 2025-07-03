//
// Created by Ofir Yanai on 03/07/2025.
//

#ifndef PARSE_HYBRID_H
#define PARSE_HYBRID_H

#include <stdint.h>

#include "redismodule.h"

#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "result_processor.h"
#include "search_ctx.h"
#include "search_options.h"
#include "hybrid_scoring.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct AggregationPipeline {
  /* plan containing the logical sequence of steps */
  AGGPlan ap;

  /** Context for iterating over the queries themselves */
  QueryProcessingCtx qctx;

  /** Context, owned by request */
  RedisSearchCtx *sctx;

  /** Flags indicating current execution state */
  uint32_t stateflags;

  /** Flags controlling query output */
  uint32_t reqflags;

  /** Fields to be output and otherwise processed */
  FieldList outFields;

} AggregationPipeline;


typedef struct {
  arrayof(AREQ) requests;
  size_t nrequests;
  AggregationPipeline merge;
  HybridScoringContext combineCtx;
} HybridRequest;


static void HybridRequest_Free(HybridRequest *hybridRequest) {
  if (!hybridRequest) return;

  if (hybridRequest->combineCtx.scoringType == HYBRID_SCORING_LINEAR &&
      hybridRequest->combineCtx.linearCtx.linearWeights) {
    rm_free(hybridRequest->combineCtx.linearCtx.linearWeights);
  }
  for (size_t i = 0; i < hybridRequest->nrequests; i++) {
    AREQ_Free(&hybridRequest->requests[i]);
  }
  array_free(hybridRequest->requests);
  rm_free(hybridRequest);
}

int execHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Function for parsing hybrid request parameters - exposed for testing
HybridRequest* parseHybridRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 RedisSearchCtx *sctx, QueryError *status);

#ifdef __cplusplus
}
#endif

#endif //PARSE_HYBRID_H
