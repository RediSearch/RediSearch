/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct PLN_DistributeStep {
  PLN_BaseStep base;
  RLookup lk;
  AGGPlan *plan;
  PLN_GroupStep **oldSteps;  // Old step which this distribute breaks down
#ifdef __cplusplus
  typedef std::vector<const char *> SerializedArray;
  SerializedArray *serialized;
#else
  void *serialized;
#endif
  BlkAlloc alloc;
} PLN_DistributeStep;

int AGGPLN_Distribute(AGGPlan *src, QueryError *status);

typedef struct {
  // Arguments to upstream FT.AGGREGATE
  const char **serialized;
  // Length of those arguments
  size_t nserialized;
  // The lookup structure containing the fields that are to be received from upstream
  RLookup *lookup;
} AREQDIST_UpstreamInfo;

/**
 * Builds the static portion of the distributed pipeline
 * @param r the request
 * @param[out] us upstream parameters
 * @param status if there is an error
 */
int AREQ_BuildDistributedPipeline(AREQ *r, AREQDIST_UpstreamInfo *us, QueryError *status);

#ifdef __cplusplus
}
#endif
