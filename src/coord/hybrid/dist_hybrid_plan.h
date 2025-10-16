/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "hybrid/hybrid_request.h"
#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Builds the static portion of the distributed pipeline
 * @param hreq the hybrid request
 * @param hybridParams pipeline parameters needed for building the pipeline
 * @param[out] lookups array to populate with lookups for each subquery
 * @param status if there is an error
 */
arrayof(char*) HybridRequest_BuildDistributedPipeline(HybridRequest *hreq, HybridPipelineParams *hybridParams, RLookup **lookups, QueryError *status);

#ifdef __cplusplus
}
#endif
