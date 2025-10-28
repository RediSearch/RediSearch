/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
  arrayof(char*) serialized;
  BlkAlloc alloc;
} PLN_DistributeStep;

int AGGPLN_Distribute(AGGPlan *src, QueryError *status);

typedef struct {
  // Arguments to upstream FT.AGGREGATE
  arrayof(char*) serialized;
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
