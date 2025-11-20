/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "aggregate.h"
#include "aggregate_plan.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "result_processor.h"
#include "query.h"
#include "rmalloc.h"
#include "util/arr.h"

/**
 * Count total results for a cursor query using an independent counting pipeline.
 *
 * This function creates a completely independent counting pipeline that:
 * 1. Rebuilds the query iterator from the AST (to get a fresh iterator)
 * 2. Initializes a minimal aggregation plan (just the ROOT step for lookup)
 * 3. Builds the query part (RPQueryIterator + scorers/filters)
 * 4. Manually adds RPCounter to count results efficiently
 * 5. Executes the pipeline and returns the count
 *
 * The approach uses RPCounter (from Rust) which internally loops through all
 * upstream results and counts them without storing them in memory.
 *
 * @param req The original AREQ (will NOT be modified)
 * @return The total count of results, or 0 on error
 */
uint32_t countTotalResults(AREQ *req) {
  QueryError status = QueryError_Default();
  uint32_t totalCount = 0;
  Pipeline counting_pipeline = {0};
  QueryIterator *counting_rootiter = NULL;

  // Check if AST root is valid
  if (!req->ast.root) {
    goto cleanup;
  }

  // Step 1: Save the AST's metricRequests array and create a temporary empty one
  // This prevents QAST_Iterate from modifying the original array
  MetricRequest *saved_metricRequests = req->ast.metricRequests;
  req->ast.metricRequests = NULL;

  // Step 2: Create a fresh query iterator from the AST
  // We need a separate iterator because the original one is owned by the main pipeline
  counting_rootiter = QAST_Iterate(&req->ast, &req->searchopts, req->sctx,
                                   req->reqflags, &status);

  // Restore the original metricRequests array
  // Free the temporary array if any was created
  if (req->ast.metricRequests) {
    array_free(req->ast.metricRequests);
  }
  req->ast.metricRequests = saved_metricRequests;

  if (!counting_rootiter || QueryError_HasError(&status)) {
    goto cleanup;
  }

  // Step 2: Initialize the counting pipeline
  Pipeline_Initialize(&counting_pipeline, req->reqConfig.timeoutPolicy, &status);
  counting_pipeline.qctx.resultLimit = UINT32_MAX;  // No limit for counting

  // Step 3: Initialize an independent aggregation plan directly in the pipeline
  // Pipeline_BuildQueryPart requires the plan to have a lookup structure
  AGPLN_Init(&counting_pipeline.ap);

  // Step 4: Build the query part with the fresh iterator
  // This creates the RPQueryIterator and any scorers/filters from the query
  QueryPipelineParams query_params = {
    .common = {
      .sctx = req->sctx,
      .reqflags = req->reqflags,  // Use original flags for query part
      .optimizer = req->optimizer,
      .scoreAlias = req->searchopts.scoreAlias,
    },
    .ast = &req->ast,
    .rootiter = counting_rootiter,
    .slotRanges = NULL,  // Don't need slot filtering for counting
    .querySlots = NULL,
    .slotsVersion = 0,
    .scorerName = req->searchopts.scorerName,
    .reqConfig = &req->reqConfig,
  };

  Pipeline_BuildQueryPart(&counting_pipeline, &query_params);
  counting_rootiter = NULL;  // Ownership transferred to pipeline

  if (QueryError_HasError(&status)) {
    goto cleanup;
  }

  // Step 5: Manually add RPCounter to the pipeline
  // We don't use Pipeline_BuildAggregationPart because it has complex logic
  // that can cause infinite loops. We just need a simple counter.
  ResultProcessor *counter = RPCounter_New();
  counter->upstream = counting_pipeline.qctx.endProc;
  counter->parent = &counting_pipeline.qctx;
  counting_pipeline.qctx.endProc = counter;

  // Step 6: Execute the counting pipeline
  ResultProcessor *rp = counting_pipeline.qctx.endProc;
  if (!rp) {
    goto cleanup;
  }

  // Call Next() once - RPCounter will internally loop through all upstream results
  // and return EOF immediately after counting them all
  SearchResult r = {0};
  int rc = rp->Next(rp, &r);

  SearchResult_Destroy(&r);

  if (rc != RS_RESULT_EOF) {
    goto cleanup;
  }

  // The totalResults field will be populated by the root processor (RPQueryIterator)
  // as RPCounter pulls results from upstream
  totalCount = counting_pipeline.qctx.totalResults;

cleanup:
  // Clean up the counting pipeline (this frees all result processors)
  QITR_FreeChain(&counting_pipeline.qctx);

  // Clean up the aggregation plan
  // The plan only contains the ROOT step (added by AGPLN_Init), no other steps
  AGPLN_FreeSteps(&counting_pipeline.ap);

  // Free the counting iterator if it wasn't transferred to the pipeline
  if (counting_rootiter) {
    counting_rootiter->Free(counting_rootiter);
  }

  QueryError_ClearError(&status);

  return totalCount;
}

