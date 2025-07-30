/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "parse_hybrid.h"

#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "aggregate/aggregate.h"
#include "vector_query_utils.h"
#include "query_error.h"
#include "spec.h"
#include "param.h"
#include "resp3.h"
#include "rmalloc.h"

#include "rmutil/args.h"
#include "rmutil/rm_assert.h"
#include "util/references.h"
#include "info/info_redis/threads/current_thread.h"

static int parseSearchSubquery(ArgsCursor *ac, AREQ *searchRequest, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "No query string provided for SEARCH");
    return REDISMODULE_ERR;
  }

  searchRequest->query = AC_GetStringNC(ac, NULL);
  AGPLN_Init(AREQ_AGGPlan(searchRequest));

  RSSearchOptions *searchOpts = &searchRequest->searchopts;
  RSSearchOptions_Init(searchOpts);

  // Currently only SCORER is possible in SEARCH. Maybe will add support for SORTBY and others later
  ACArgSpec querySpecs[] = {
    {.name = "SCORER", .type = AC_ARGTYPE_STRING, .target = &searchOpts->scorerName},
    {NULL}
  };

  // Parse all querySpecs until we hit VSIM, unknown argument, or the end
  while (!AC_IsAtEnd(ac)) {
    ACArgSpec *errSpec = NULL;
    int rv = AC_ParseArgSpec(ac, querySpecs, &errSpec);
    if (rv == AC_OK) {
      continue;
    }

    if (rv != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errSpec->name, rv);
      return REDISMODULE_ERR;
    }

    // AC_ERR_ENOENT - check if it's VSIM or just an unknown argument
    const char *cur;
    if (AC_GetString(ac, &cur, NULL, AC_F_NOADVANCE) == AC_OK && !strcasecmp("VSIM", cur)) {
      // Hit VSIM, we're done with search options
      return REDISMODULE_OK;
    }

    // Unknown argument that's not VSIM - this is an error
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in SEARCH", cur);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static int parseKNNClause(ArgsCursor *ac, ParsedVectorQuery *pvq, QueryError *status) {
  AC_Advance(ac);
  // Try to get number of parameters
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count");
    return REDISMODULE_ERR;
  } else if (params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid parameter coun");
    return REDISMODULE_ERR;
  }

  bool hasK = false;  // codespell:ignore
  bool hasEF = false;
  bool hasYieldDistanceAs = false;
  const char *current;
  for (int i=0; i<params; i+=2) {
    if (AC_GetString(ac, &current, NULL, AC_F_NOADVANCE) != AC_OK) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "Missing parameter");
        return REDISMODULE_ERR;
      } else {
        QueryError_SetError(status, QUERY_EGENERIC, "Generic Error");
        return REDISMODULE_ERR;
      }
    }
    if (!strcasecmp(current, "K")){
      if (hasK) { // codespell:ignore
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate K parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        long long kValue;
        if (AC_GetLongLong(ac, &kValue, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value");
          return REDISMODULE_ERR;
        }
        pvq->k = (size_t)kValue;
        hasK = true;  // codespell:ignore
      }
    } else if (!strcasecmp(current, "EF_RUNTIME")) {
      if (hasEF) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EF_RUNTIME parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        const char *value;
        if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EF_RUNTIME value");
          return REDISMODULE_ERR;
        }
        // Add as QueryAttribute (for query node processing)
        QueryAttribute attr = {
          .name = VECSIM_EFRUNTIME,
          .namelen = strlen(VECSIM_EFRUNTIME),
          .value = rm_strdup(value),
          .vallen = strlen(value)
        };
        pvq->attributes = array_ensure_append_1(pvq->attributes, attr);
        hasEF = true;
      }
    } else if (!strcasecmp(current, "YIELD_DISTANCE_AS")) {
      if (hasYieldDistanceAs) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        const char *value;
        if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Missing distance field name");
          return REDISMODULE_ERR;
        }

        // As QueryAttribute (for query node processing)
        QueryAttribute attr = {
          .name = YIELD_DISTANCE_ATTR,
          .namelen = strlen(YIELD_DISTANCE_ATTR),
          .value = rm_strdup(value),
          .vallen = strlen(value)
        };
        pvq->attributes = array_ensure_append_1(pvq->attributes, attr);
        hasYieldDistanceAs = true;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in KNN", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasK) { // codespell:ignore
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing K parameter");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}


static int parseRangeClause(ArgsCursor *ac, ParsedVectorQuery *pvq, QueryError *status) {
  AC_Advance(ac);
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count");
    return REDISMODULE_ERR;
  } else if (params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid parameter coun");
    return REDISMODULE_ERR;
  }
  bool hasRadius = false;
  bool hasEpsilon = false;
  bool hasYieldDistanceAs = false;
  const char *current;
  for (int i=0; i<params; i+=2) {
    if (AC_GetString(ac, &current, NULL, AC_F_NOADVANCE) != AC_OK) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "Missing parameter");
        return REDISMODULE_ERR;
      } else {
        QueryError_SetError(status, QUERY_EGENERIC, "Generic Error");
        return REDISMODULE_ERR;
      }
    }
    if (!strcasecmp(current, "RADIUS")) {
      if (hasRadius) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate RADIUS parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        double radiusValue;
        if (AC_GetDouble(ac, &radiusValue, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid RADIUS value");
          return REDISMODULE_ERR;
        }
        pvq->radius = radiusValue;
        hasRadius = true;
      }
    } else if (!strcasecmp(current, "EPSILON")) {
      if (hasEpsilon) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EPSILON parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        const char *value;
        if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EPSILON value");
          return REDISMODULE_ERR;
        }
        // Add as QueryAttribute (for query node processing)
        QueryAttribute attr = {
          .name = VECSIM_EPSILON,
          .namelen = strlen(VECSIM_EPSILON),
          .value = rm_strdup(value),
          .vallen = strlen(value)
        };
        pvq->attributes = array_ensure_append_1(pvq->attributes, attr);
        hasEpsilon = true;
      }
    } else if (!strcasecmp(current, "YIELD_DISTANCE_AS")) {
      if (hasYieldDistanceAs) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        const char *value;
        if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Missing distance field name");
          return REDISMODULE_ERR;
        }

        // As QueryAttribute (for query node processing)
        QueryAttribute attr = {
          .name = YIELD_DISTANCE_ATTR,
          .namelen = strlen(YIELD_DISTANCE_ATTR),
          .value = rm_strdup(value),
          .vallen = strlen(value)
        };
        pvq->attributes = array_ensure_append_1(pvq->attributes, attr);
        hasYieldDistanceAs = true;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RANGE", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasRadius) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing RADIUS parameter");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
static int parseFilterClause(ArgsCursor *ac, AREQ *vectorRequest, QueryError *status) {
  // FILTER is in our scope, advance and process it
  AC_Advance(ac);
  vectorRequest->query = AC_GetStringNC(ac, NULL);
  return REDISMODULE_OK;
}

static int parseVectorSubquery(ArgsCursor *ac, AREQ *vectorRequest, QueryError *status) {
  const char *current;
  if (AC_GetString(ac, &current, NULL, AC_F_NOADVANCE) != AC_OK || strcasecmp("VSIM", current)) {
    QueryError_SetError(status, QUERY_ESYNTAX, "VSIM parameter is required");
    return REDISMODULE_ERR;
  }
  AC_Advance(ac);

  // Initialize aggregation plan for vector request
  AGPLN_Init(AREQ_AGGPlan(vectorRequest));

  // Allocate ParsedVectorQuery
  ParsedVectorQuery *pvq = rm_calloc(1, sizeof(ParsedVectorQuery));

  // Parse vector field and blob
  if (AC_GetString(ac, &pvq->fieldName, NULL, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector field name");
    goto error;
  }

  const char *vectorParam;
  if (AC_GetString(ac, &vectorParam, NULL, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector blob");
    goto error;
  }

  if (vectorParam[0] == '$') {
    // PARAMETER CASE: store parameter name for later resolution
    vectorParam++;  // Skip '$'
    pvq->vector = vectorParam;  // Parameter name
    pvq->vectorLen = strlen(vectorParam);
    pvq->isParameter = true;
  } else {
    // DIRECT VECTOR CASE: store the actual vector data
    pvq->vector = vectorParam;  // Actual vector data
    pvq->vectorLen = strlen(vectorParam);
    pvq->isParameter = false;
  }
  if (AC_IsAtEnd(ac)) goto final;

  // Initialize QueryAttribute array for attributes like YIELD_DISTANCE_AS
  pvq->attributes = array_new(QueryAttribute, 0);

  if (AC_GetString(ac, &current, NULL, AC_F_NOADVANCE) != AC_OK && !AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Unknown parameter after VSIM");
    goto error;
  }

  if (!strcasecmp(current, "KNN")) {
    if (parseKNNClause(ac, pvq, status) != REDISMODULE_OK) {
      goto error;
    }
    pvq->type = VECSIM_QT_KNN;
    if (AC_IsAtEnd(ac)) goto final;
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
  } else if (!strcasecmp(current, "RANGE")) {
    if (parseRangeClause(ac, pvq, status) != REDISMODULE_OK) {
      goto error;
    }
    pvq->type = VECSIM_QT_RANGE;
    if (AC_IsAtEnd(ac)) goto final;
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
  }

  // Check for optional FILTER clause - parameter may not be in our scope
  if (!strcasecmp(current, "FILTER")) {
    if (parseFilterClause(ac, vectorRequest, status) != REDISMODULE_OK) {
      goto error;
    }
  }
  // If not FILTER, the parameter may be for the next parsing function (COMBINE, etc.)

  goto final;

final:
  if (!vectorRequest->query) {  // meaning there is no filter clause
    vectorRequest->query = "*";
  }
  vectorRequest->parsedVectorQuery = pvq;
  return REDISMODULE_OK;

error:
  ParsedVectorQuery_Free(pvq);
  return REDISMODULE_ERR;
}

/**
 * Parse COMBINE clause parameters for hybrid scoring configuration.
 *
 * Supports LINEAR (requires HYBRID_REQUEST_NUM_SUBQUERIES weight values) and RRF (optional K and WINDOW parameters).
 * Defaults to RRF if no method specified.
 *
 * @param ac Arguments cursor positioned after "COMBINE"
 * @param combineCtx Hybrid scoring context to populate
 * @param status Output parameter for error reporting
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
static int parseCombine(ArgsCursor *ac, HybridScoringContext *combineCtx, QueryError *status) {
  // Check if a specific method is provided
  if (AC_AdvanceIfMatch(ac, "LINEAR")) {
    combineCtx->scoringType = HYBRID_SCORING_LINEAR;
  } else if (AC_AdvanceIfMatch(ac, "RRF")) {
    combineCtx->scoringType = HYBRID_SCORING_RRF;
  } else {
    combineCtx->scoringType = HYBRID_SCORING_RRF;
  }

  // Parse parameters based on scoring type
  if (combineCtx->scoringType == HYBRID_SCORING_LINEAR) {
    combineCtx->linearCtx.linearWeights = rm_calloc(HYBRID_REQUEST_NUM_SUBQUERIES, sizeof(double));
    combineCtx->linearCtx.numWeights = HYBRID_REQUEST_NUM_SUBQUERIES;

    // Parse the weight values directly
    for (size_t i = 0; i < HYBRID_REQUEST_NUM_SUBQUERIES; i++) {
      double weight;
      if (AC_GetDouble(ac, &weight, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing or invalid weight value in LINEAR weights");
        goto error;
      }
      combineCtx->linearCtx.linearWeights[i] = weight;
    }
  } else if (combineCtx->scoringType == HYBRID_SCORING_RRF) {
    // For RRF, we need k and window parameters
    ArgsCursor params = {0};
    int rv = AC_GetVarArgs(ac, &params);

    // Initialize with defaults
    combineCtx->rrfCtx.k = 1;
    combineCtx->rrfCtx.window = 20;

    if (rv == AC_OK) {
      // Parameters were provided
      if (params.argc % 2 != 0) {
        QueryError_SetError(status, QUERY_ESYNTAX, "RRF parameters must be in name-value pairs");
        goto error;
      }

      // Parse the specified parameters
      while (!AC_IsAtEnd(&params)) {
        const char *paramName = AC_GetStringNC(&params, NULL);
        if (!paramName) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter name in RRF");
          goto error;
        }

        if (strcasecmp(paramName, "K") == 0) {
          double k;
          if (AC_GetDouble(&params, &k, 0) != AC_OK) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value in RRF");
            goto error;
          }
          combineCtx->rrfCtx.k = k;
        } else if (strcasecmp(paramName, "WINDOW") == 0) {
          long long window;
          if (AC_GetLongLong(&params, &window, 0) != AC_OK || window <= 0) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid WINDOW value in RRF");
            goto error;
          }
          combineCtx->rrfCtx.window = window;
        } else {
          QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RRF", paramName);
          goto error;
        }
      }
    }
  }

  return REDISMODULE_OK;

error:
  HybridScoringContext_Free(combineCtx);
  return REDISMODULE_ERR;
}


// Copy request configuration from source to destination
static void copyRequestConfig(RequestConfig *dest, const RequestConfig *src) {
  dest->queryTimeoutMS = src->queryTimeoutMS;
  dest->dialectVersion = src->dialectVersion;
  dest->timeoutPolicy = src->timeoutPolicy;
  dest->printProfileClock = src->printProfileClock;
  dest->BM25STD_TanhFactor = src->BM25STD_TanhFactor;
}

/**
 * Parse FT.HYBRID command arguments and build a complete HybridRequest structure.
 *
 * Expected format: FT.HYBRID <index> SEARCH <query> [SCORER <scorer>] VSIM <vector_args>
 *                  [COMBINE <method> [params]] [aggregation_options]
 *
 * @param ctx Redis module context
 * @param argv Command arguments array (starting with "FT.HYBRID")
 * @param argc Number of arguments in argv
 * @param sctx Search context for the index (takes ownership)
 * @param indexname Name of the index to search
 * @param status Output parameter for error reporting
 * @return HybridRequest* on success, NULL on error
 *
 * @note Takes ownership of sctx. Exposed for testing.
 */
HybridRequest* parseHybridCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 RedisSearchCtx *sctx, const char *indexname,
                                 QueryError *status) {
  AREQ *searchRequest = AREQ_New();
  AREQ *vectorRequest = AREQ_New();

  HybridPipelineParams *hybridParams = rm_calloc(1, sizeof(HybridPipelineParams));
  hybridParams->scoringCtx = rm_calloc(1, sizeof(HybridScoringContext));
  hybridParams->scoringCtx->scoringType = HYBRID_SCORING_RRF;
  hybridParams->scoringCtx->rrfCtx = (HybridRRFContext) {
    .k = 1,
    .window = 20
  };

  RedisModuleCtx *ctx1 = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(ctx1, RedisModule_GetSelectedDb(ctx));
  searchRequest->sctx = NewSearchCtxC(ctx1, indexname, true);
  RedisModuleCtx *ctx2 = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(ctx2, RedisModule_GetSelectedDb(ctx));
  vectorRequest->sctx = NewSearchCtxC(ctx2, indexname, true);

  // Individual variables used for parsing the tail of the command
  Pipeline *mergePipeline = NULL;
  uint32_t mergeReqflags = 0;
  RequestConfig mergeReqConfig = RSGlobalConfig.requestConfigParams;
  RSSearchOptions mergeSearchopts = {0};
  CursorConfig mergeCursorConfig = {0};
  size_t mergeMaxSearchResults = RSGlobalConfig.maxSearchResults;
  size_t mergeMaxAggregateResults = RSGlobalConfig.maxAggregateResults;

  AREQ **requests = NULL;
  searchRequest->reqflags |= QEXEC_F_IS_AGGREGATE;
  vectorRequest->reqflags |= QEXEC_F_IS_AGGREGATE;

  ArgsCursor ac;
  ArgsCursor_InitRString(&ac, argv + 2, argc - 2);
  if (AC_IsAtEnd(&ac) || !AC_AdvanceIfMatch(&ac, "SEARCH")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "SEARCH parameter is required");
    goto error;
  }

  if (parseSearchSubquery(&ac, searchRequest, status) != REDISMODULE_OK) {
    goto error;
  }

  if (parseVectorSubquery(&ac, vectorRequest, status) != REDISMODULE_OK) {
    goto error;
  }

  // Look for optional COMBINE parameter
  if (AC_AdvanceIfMatch(&ac, "COMBINE")) {
    if (parseCombine(&ac, hybridParams->scoringCtx, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // Save the current position to determine remaining arguments for the merge part
  const int remainingOffset = (int) ac.offset;
  const int remainingArgs = argc - 2 - remainingOffset;

  // If there are remaining arguments, parse them into the aggregate plan
  bool hasMerge = false;
  if (remainingArgs > 0) {
    hasMerge = true;

    mergePipeline = rm_calloc(1, sizeof(Pipeline));
    AGPLN_Init(&mergePipeline->ap);
    RSSearchOptions_Init(&mergeSearchopts);

    ParseAggPlanContext papCtx = {
      .plan = &mergePipeline->ap,
      .reqflags = &mergeReqflags,
      .reqConfig = &mergeReqConfig,
      .searchopts = &mergeSearchopts,
      .prefixesOffset = NULL,               // Invalid in FT.HYBRID
      .cursorConfig = &mergeCursorConfig,   // TODO: Confirm if this is supported
      .requiredFields = NULL,               // Invalid in FT.HYBRID
      .maxSearchResults = &mergeMaxSearchResults,
      .maxAggregateResults = &mergeMaxAggregateResults
    };
    if (parseAggPlan(&papCtx, &ac, status) != REDISMODULE_OK) {
      goto error;
    }

    if (mergeSearchopts.params) {
      searchRequest->searchopts.params = Param_DictClone(mergeSearchopts.params);
      vectorRequest->searchopts.params = Param_DictClone(mergeSearchopts.params);
      Param_DictFree(mergeSearchopts.params);
    }

    // Copy request configuration using the helper function
    copyRequestConfig(&searchRequest->reqConfig, &mergeReqConfig);
    copyRequestConfig(&vectorRequest->reqConfig, &mergeReqConfig);

    // Copy max results limits
    searchRequest->maxSearchResults = mergeMaxSearchResults;
    searchRequest->maxAggregateResults = mergeMaxAggregateResults;
    vectorRequest->maxSearchResults = mergeMaxSearchResults;
    vectorRequest->maxAggregateResults = mergeMaxAggregateResults;

    if (QAST_EvalParams(&vectorRequest->ast, &vectorRequest->searchopts, 2, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // Create the hybrid request with proper structure
  requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  array_ensure_append_1(requests, searchRequest);
  array_ensure_append_1(requests, vectorRequest);

  // Apply context to each request
  AREQ *req = NULL;
  array_foreach(requests, req, {
    if (AREQ_ApplyContext(req, req->sctx, status) != REDISMODULE_OK) {
      goto error;
    }
  });

  HybridRequest *hybridRequest = HybridRequest_New(requests, HYBRID_REQUEST_NUM_SUBQUERIES);
  hybridRequest->hybridParams = hybridParams;

  Pipeline_Initialize(mergePipeline, requests[0]->pipeline.qctx.timeoutPolicy, &hybridRequest->tailPipelineError);

  // thread safe context
  const AggregationPipelineParams params = {
      .common =
          {
              .sctx = sctx,  // should be a separate context?
              .reqflags = hasMerge ? mergeReqflags : 0,
              .optimizer = NULL,  // is it?
          },
      .outFields = NULL,
      .maxResultsLimit = mergeMaxAggregateResults,
      .language = searchRequest->searchopts.language,
  };

  hybridParams->aggregationParams = params;
  hybridParams->synchronize_read_locks = true;

  if (hasMerge) {
    // Create and transfer the pipeline
    if (hybridRequest->tailPipeline) {
      Pipeline_Clean(hybridRequest->tailPipeline);
      rm_free(hybridRequest->tailPipeline);
    }

    hybridRequest->tailPipeline = mergePipeline;
    mergePipeline = NULL;  // Prevent double free
  }

  return hybridRequest;

error:
  if (searchRequest) {
    if (searchRequest->sctx) {
      RedisModuleCtx *thctx = searchRequest->sctx->redisCtx;
      SearchCtx_Free(searchRequest->sctx);
      if (thctx) {
        RedisModule_FreeThreadSafeContext(thctx);
      }
      searchRequest->sctx = NULL;
    }
    AREQ_Free(searchRequest);
  }

  if (vectorRequest) {
    if (vectorRequest->sctx) {
      RedisModuleCtx *thctx = vectorRequest->sctx->redisCtx;
      SearchCtx_Free(vectorRequest->sctx);
      if (thctx) {
        RedisModule_FreeThreadSafeContext(thctx);
      }
      vectorRequest->sctx = NULL;
    }
    AREQ_Free(vectorRequest);
  }

  if (requests) {
    array_free(requests);
  }

  if (hybridParams) {
    HybridScoringContext_Free(hybridParams->scoringCtx);
    rm_free(hybridParams);
  }

  if (mergePipeline) {
    Pipeline_Clean(mergePipeline);
    rm_free(mergePipeline);
  }

  return NULL;
}


static int HREQ_BuildPipelineAndExecute(HybridRequest *hreq, RedisModuleCtx *ctx, QueryError *status) {
  RedisSearchCtx *sctx1 = hreq->requests[0]->sctx;
  RedisSearchCtx *sctx2 = hreq->requests[1]->sctx;

  if (RunInThread()) {
    // TODO: Implement multi-threaded execution
    return REDISMODULE_ERR;
  } else {
    // Lock both specs for read
    // RedisSearchCtx_LockSpecRead(sctx1);
    // RedisSearchCtx_LockSpecRead(sctx2);

    int result = REDISMODULE_OK;
    // Build the pipeline and execute
    if (HybridRequest_BuildPipeline(hreq, hreq->hybridParams) != REDISMODULE_OK) {
      result = REDISMODULE_ERR;
    } else {
      HybridRequest_Execute(hreq, ctx);
    }

    // Always unlock both specs
    // RedisSearchCtx_UnlockSpec(sctx1);
    // RedisSearchCtx_UnlockSpec(sctx2);
    CurrentThread_ClearIndexSpec();

    return result;
  }
}

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError status = {0};
    QueryError_SetWithUserDataFmt(&status, QUERY_ENOINDEX, "No such index", " %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
  CurrentThread_SetIndexSpec(spec_ref);

  QueryError status = {0};

  HybridRequest *hybridRequest = parseHybridCommand(ctx, argv, argc, sctx, indexname, &status);
  if (!hybridRequest) {
    goto error;
  }

  if (HREQ_BuildPipelineAndExecute(hybridRequest, ctx, &status) != REDISMODULE_OK) {
    goto error;
  }

  StrongRef_Release(spec_ref);
  HybridRequest_Free(hybridRequest);
  return REDISMODULE_OK;

error:
  RS_LOG_ASSERT(QueryError_HasError(&status), "Hybrid query parsing error");

  // Clear the current thread's index spec if it was set
  CurrentThread_ClearIndexSpec();

  // Release our strong reference to the spec if it was acquired
  if (spec_ref.rm) {
    StrongRef_Release(spec_ref);
  }

  // Free the search context
  SearchCtx_Free(sctx);

  return QueryError_ReplyAndClear(ctx, &status);
}
