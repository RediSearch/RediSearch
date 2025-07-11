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

static int parseVectorSubquery(ArgsCursor *ac, AREQ *vectorRequest, QueryError *status) {
  // Check if VSIM parameter is present
  if (!AC_AdvanceIfMatch(ac, "VSIM")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "VSIM parameter is required");
    return REDISMODULE_ERR;
  }

  // Advance the cursor until we encounter one of the specified keywords or reach the end
  while (!AC_IsAtEnd(ac)) {
    const char *cur;
    if (AC_GetString(ac, &cur, NULL, AC_F_NOADVANCE) != AC_OK) {
      break;
    }

    // Check if current argument is one of the keywords that should stop parsing
    if (!strcasecmp(cur, "COMBINE") ||
        !strcasecmp(cur, "LOAD") ||
        !strcasecmp(cur, "GROUPBY") ||
        !strcasecmp(cur, "APPLY") ||
        !strcasecmp(cur, "SORTBY") ||
        !strcasecmp(cur, "FILTER") ||
        !strcasecmp(cur, "LIMIT") ||
        !strcasecmp(cur, "PARAMS") ||
        !strcasecmp(cur, "EXPLAINSCORE") ||
        !strcasecmp(cur, "TIMEOUT")) {
      // Found a keyword that should stop parsing, don't advance past it
      break;
    }

    // Not a stopping keyword, advance to next argument
    AC_Advance(ac);
  }

  // TODO: Parse additional vector parameters (method, FILTER, etc.)
  return REDISMODULE_OK;
}

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
    // For LINEAR, we expect exactly 2 weight values
    combineCtx->linearCtx.linearWeights = rm_calloc(2, sizeof(double));
    combineCtx->linearCtx.numWeights = 2;

    // Parse the two weight values directly
    for (size_t i = 0; i < 2; i++) {
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


HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests) {
  HybridRequest *req = rm_calloc(1, sizeof(*req));
  req->requests = requests;
  req->nrequests = nrequests;

  // Initialize error tracking for each individual request plus the tail pipeline
  req->errors = array_new(QueryError, nrequests);

  // Initialize the tail pipeline that will merge results from all requests
  AGPLN_Init(&req->tail.ap);
  QueryError_Init(&req->tailError);
  Pipeline_Initialize(&req->tail, requests[0]->pipeline.qctx.timeoutPolicy, &req->tailError);

  // Initialize pipelines for each individual request
  for (size_t i = 0; i < nrequests; i++) {
    QueryError_Init(&req->errors[i]);
    Pipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &req->errors[i]);
  }
  return req;
}

int HybridRequest_BuildPipeline(HybridRequest *req) { return REDISMODULE_OK; }

void HybridRequest_Free(HybridRequest *req) {
  if (!req) return;

  // Free all individual AREQ requests
  for (size_t i = 0; i < req->nrequests; i++) {
    // Check if we need to manually free the thread-safe context
    // if (req->requests[i]->sctx && req->requests[i]->sctx->redisCtx &&
    //     !(req->requests[i]->reqflags & QEXEC_F_IS_CURSOR)) {
    if (req->requests[i]->sctx && req->requests[i]->sctx->redisCtx) {

      // Free the search context
      RedisModuleCtx *thctx = req->requests[i]->sctx->redisCtx;
      RedisSearchCtx *sctx = req->requests[i]->sctx;
      SearchCtx_Free(sctx);
      // Free the thread-safe context
      if (thctx) {
        RedisModule_FreeThreadSafeContext(thctx);
      }
      req->requests[i]->sctx = NULL;
    }

    AREQ_Free(req->requests[i]);
  }

  // Free the scoring context resources
  HybridScoringContext_Free(req->hybridParams->scoringCtx);

  // Free the aggregation search context
  if(req->hybridParams->aggregation.common.sctx) {
    SearchCtx_Free(req->hybridParams->aggregation.common.sctx);
  }
  // Free the hybrid parameters
  rm_free(req->hybridParams);

  // Free the arrays and tail pipeline
  array_free(req->requests);
  array_free(req->errors);

  // Clean up the tail pipeline
  Pipeline_Clean(&req->tail);

  rm_free(req);
}


HybridRequest* parseHybridRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
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

  AREQ *mergeAreq = NULL;
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
    mergeAreq = AREQ_New();

    AGPLN_Init(AREQ_AGGPlan(mergeAreq));
    RSSearchOptions_Init(&mergeAreq->searchopts);
    if (parseAggPlan(mergeAreq, &ac, status) != REDISMODULE_OK) {
      goto error;
    }

    mergeAreq->protocol = is_resp3(ctx) ? 3 : 2;

    searchRequest->searchopts.params = Param_DictClone(mergeAreq->searchopts.params);
    vectorRequest->searchopts.params = Param_DictClone(mergeAreq->searchopts.params);

    if (QAST_EvalParams(&vectorRequest->ast, &vectorRequest->searchopts, 2, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // TODO: copy sctx to searchRequest->sctx ?
  if (AREQ_ApplyContext(searchRequest, searchRequest->sctx, status) != REDISMODULE_OK) {
    goto error;
  }

  // Create the hybrid request with proper structure
  requests = array_new(AREQ*, 2);
  array_ensure_append_1(requests, searchRequest);
  array_ensure_append_1(requests, vectorRequest);

  HybridRequest *hybridRequest = HybridRequest_New(requests, 2);
  hybridRequest->hybridParams = hybridParams;

  // thread safe context
  const AggregationPipelineParams params = {
      .common =
          {
              .pln = NULL,  // I think. should be copied in HybridRequest_BuildPipeline
              .sctx = sctx,  // should be a separate context?
              .reqflags = hasMerge ? mergeAreq->reqflags : 0,
              .optimizer = NULL,  // is it?
          },
      .outFields = NULL,
      .maxResultsLimit = hasMerge ? mergeAreq->maxAggregateResults : RSGlobalConfig.maxAggregateResults,
      .language = searchRequest->searchopts.language,
  };

  hybridParams->aggregation = params;
  hybridParams->synchronize_read_locks = true;

  if (hasMerge) {
    // Clean up the existing plan first to avoid memory leaks
    AGPLN_FreeSteps(&hybridRequest->tail.ap);

    // Use memcpy to create a deep copy of the aggregation plan
    memcpy(&hybridRequest->tail.ap, &mergeAreq->pipeline.ap, sizeof(AGGPlan));

    // Clear the source plan's pointers to avoid double free
    // but keep the structure intact
    memset(&mergeAreq->pipeline.ap, 0, sizeof(AGGPlan));
  }

  if (mergeAreq) {
    AREQ_Free(mergeAreq);
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

  if (mergeAreq) {
    AREQ_Free(mergeAreq);
  }
  if (requests) {
    array_free(requests);
  }

  if (hybridParams) {
    HybridScoringContext_Free(hybridParams->scoringCtx);
    rm_free(hybridParams);
  }

  return NULL;
}

int execHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

  HybridRequest *hybridRequest = parseHybridRequest(ctx, argv, argc, sctx, indexname, &status);
  if (!hybridRequest) {
    goto error;
  }

  if (HybridRequest_BuildPipeline(hybridRequest) != REDISMODULE_OK) {
    goto error;
  }

  // TODO: Add execute command here

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
