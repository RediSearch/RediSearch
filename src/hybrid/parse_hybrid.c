//
// Created by Ofir Yanai on 03/07/2025.
//

#include "parse_hybrid.h"

#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "aggregate/aggregate.h"
#include "query_error.h"
#include "query_node.h"
#include "spec.h"
#include "param.h"
#include "resp3.h"
#include "rmalloc.h"
#include "hiredis/sds.h"

#include "rmutil/args.h"
#include "rmutil/rm_assert.h"
#include "util/references.h"
#include "info/info_redis/threads/current_thread.h"


static int parseSearchSubquery(ArgsCursor *ac, AREQ *searchRequest, QueryError *status) {
  searchRequest->query = AC_GetStringNC(ac, NULL);
  AGPLN_Init(&searchRequest->ap);

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

static int parseKNNClause(ArgsCursor *ac, SimpleVectorQuery *svq, QueryAttribute **attributes, QueryError *status) {
  AC_Advance(ac);
  // Try to get number of parameters
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK || params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count for KNN");
    return REDISMODULE_ERR;
  }

  bool hasK = false;
  bool hasEF = false;
  bool hasYieldDistanceAs = false;
  const char *current;
  for (int i=0; i<params; i+=2) {
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
    if (!strcasecmp(current, "K")){
      if (hasK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate K parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        long long kValue;
        if (AC_GetLongLong(ac, &kValue, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value");
          return REDISMODULE_ERR;
        }
        svq->k = (size_t)kValue;
        hasK = true;
      }
    } else if (!strcasecmp(current, "EF_RUNTIME")) {
      if (hasEF) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate EF_RUNTIME parameter");
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
        *attributes = array_ensure_append_1(*attributes, attr);
        hasEF = true;
      }
    } else if (!strcasecmp(current, "YIELD_DISTANCE_AS")) {
      if (hasYieldDistanceAs) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate YIELD_DISTANCE_AS parameter");
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
        *attributes = array_ensure_append_1(*attributes, attr);
        hasYieldDistanceAs = true;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in KNN", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing K parameter");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseRangeClause(ArgsCursor *ac, SimpleVectorQuery *svq, QueryAttribute **attributes, QueryError *status) {
  AC_Advance(ac);
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK || params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count for RANGE");
    return REDISMODULE_ERR;
  }
  bool hasRadius = false;
  bool hasEpsilon = false;
  bool hasYieldDistanceAs = false;
  const char *current;
  for (int i=0; i<params; i+=2) {
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
    if (!strcasecmp(current, "RADIUS")) {
      if (hasRadius) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate RADIUS parameter");
        return REDISMODULE_ERR;
      } else {
        AC_Advance(ac);
        double radiusValue;
        if (AC_GetDouble(ac, &radiusValue, 0) != AC_OK) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid RADIUS value");
          return REDISMODULE_ERR;
        }
        svq->radius = radiusValue;
        hasRadius = true;
      }
    } else if (!strcasecmp(current, "EPSILON")) {
      if (hasEpsilon) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate EPSILON parameter");
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
        *attributes = array_ensure_append_1(*attributes, attr);
        hasEpsilon = true;
      }
    } else if (!strcasecmp(current, "YIELD_DISTANCE_AS")) {
      if (hasYieldDistanceAs) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Duplicate YIELD_DISTANCE_AS parameter");
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
        *attributes = array_ensure_append_1(*attributes, attr);
        hasYieldDistanceAs = true;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RANGE", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasRadius) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing RADIUS parameter");
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
  const char *cur;
  if (AC_GetString(ac, &cur, NULL, AC_F_NOADVANCE) != AC_OK || strcasecmp("VSIM", cur)) {
    QueryError_SetError(status, QUERY_ESYNTAX, "VSIM parameter is required");
    return REDISMODULE_ERR;
  }
  AC_Advance(ac);

  // Allocate SimpleVectorQuery
  SimpleVectorQuery *svq = rm_calloc(1, sizeof(SimpleVectorQuery));

  // Parse vector field and blob
  if (AC_GetString(ac, &svq->fieldName, NULL, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector field name");
    SimpleVectorQuery_Free(svq);
    return REDISMODULE_ERR;
  }

  const char *vectorParam;
  if (AC_GetString(ac, &vectorParam, NULL, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector blob");
    SimpleVectorQuery_Free(svq);
    return REDISMODULE_ERR;
  }
  if (vectorParam[0] != '$') {
    QueryError_SetError(status, QUERY_ESYNTAX, "Vector blob must be a parameter");
    SimpleVectorQuery_Free(svq);
    return REDISMODULE_ERR;
  }
  vectorParam++;

  // Store reference to vector parameter (like regular flow - no copy)
  svq->vector = vectorParam;  // Just reference, no copy
  svq->vectorLen = strlen(vectorParam);

  // Initialize QueryAttribute array for attributes like YIELD_DISTANCE_AS
  QueryAttribute *attributes = array_new(QueryAttribute, 0);

  const char *current;
  if (AC_GetString(ac, &current, NULL, AC_F_NOADVANCE) != AC_OK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Unknown parameter after VSIM");
    SimpleVectorQuery_Free(svq);
    array_free(attributes);
    return REDISMODULE_ERR;
  }

  if (!strcasecmp(current, "KNN")) {
    if (parseKNNClause(ac, svq, &attributes, status) != REDISMODULE_OK) {
      SimpleVectorQuery_Free(svq);
      array_free(attributes);
      return REDISMODULE_ERR;
    }
    svq->type = VECSIM_QT_KNN;
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
  } else if (!strcasecmp(current, "RANGE")) {
    if (parseRangeClause(ac, svq, &attributes, status) != REDISMODULE_OK) {
      SimpleVectorQuery_Free(svq);
      array_free(attributes);
      return REDISMODULE_ERR;
    }
    svq->type = VECSIM_QT_RANGE;
    AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
  }

  // Store attributes in SimpleVectorQuery
  svq->attributes = attributes;

  // Check for optional FILTER clause - parameter may not be in our scope
  if (!strcasecmp(current, "FILTER")) {
    if (parseFilterClause(ac, vectorRequest, status) != REDISMODULE_OK) {
      SimpleVectorQuery_Free(svq);
      return REDISMODULE_ERR;
    }
  } else {
    vectorRequest->query = "*";
  }
  // If not FILTER, the parameter may be for the next parsing function (COMBINE, etc.)

  vectorRequest->simpleVectorQuery = svq;

  return REDISMODULE_OK;
}

static int parseCombine(ArgsCursor *ac, HybridScoringContext *combineCtx, QueryError *status) {
  // Default to RRF if method not specified
  HybridScoringType scoringType = HYBRID_SCORING_RRF;

  // Check if a specific method is provided
  if (AC_AdvanceIfMatch(ac, "LINEAR")) {
    combineCtx->scoringType = HYBRID_SCORING_LINEAR;
  } else if (AC_AdvanceIfMatch(ac, "RRF")) {
    combineCtx->scoringType = HYBRID_SCORING_RRF;
  } else {
    // If no method specified, use default RRF
  }

  // Parse parameters based on scoring type
  if (combineCtx->scoringType == HYBRID_SCORING_LINEAR) {
    // For LINEAR, we need weights
    ArgsCursor params = {0};
    int rv = AC_GetVarArgs(ac, &params);
    if (rv != AC_OK) {
      QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count after LINEAR");
      return REDISMODULE_ERR;
    }

    // Each weight is a pair (name, value)
    size_t numWeights = params.argc / 2;
    if (params.argc % 2 != 0) {
      QueryError_SetError(status, QUERY_ESYNTAX, "LINEAR parameters must be in name-value pairs");
      return REDISMODULE_ERR;
    }

    // Allocate weights array
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;

    // Parse weights
    for (size_t i = 0; i < numWeights; i++) {
      // Skip the parameter name (we only care about values for now)
      const char *paramName = AC_GetStringNC(&params, NULL);
      if (!paramName) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter name in LINEAR weights");
        rm_free(combineCtx->linearCtx.linearWeights);
        return REDISMODULE_ERR;
      }

      // Get the weight value
      double weight;
      if (AC_GetDouble(&params, &weight, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing or invalid weight value in LINEAR weights");
        rm_free(combineCtx->linearCtx.linearWeights);
        return REDISMODULE_ERR;
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
        return REDISMODULE_ERR;
      }

      // Parse the specified parameters
      while (!AC_IsAtEnd(&params)) {
        const char *paramName = AC_GetStringNC(&params, NULL);
        if (!paramName) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter name in RRF");
          return REDISMODULE_ERR;
        }

        if (strcasecmp(paramName, "K") == 0) {
          double k;
          if (AC_GetDouble(&params, &k, 0) != AC_OK) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value in RRF");
            return REDISMODULE_ERR;
          }
          combineCtx->rrfCtx.k = k;
        } else if (strcasecmp(paramName, "WINDOW") == 0) {
          long long window;
          if (AC_GetLongLong(&params, &window, 0) != AC_OK || window <= 0) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid WINDOW value in RRF");
            return REDISMODULE_ERR;
          }
          combineCtx->rrfCtx.window = window;
        } else {
          QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RRF", paramName);
          return REDISMODULE_ERR;
        }
      }
    }
  }

  return REDISMODULE_OK;
}

HybridRequest* parseHybridRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 RedisSearchCtx *sctx, QueryError *status) {
  AREQ *searchRequest = AREQ_New();
  AREQ *vectorRequest = AREQ_New();
  AREQ *mergeAreq = NULL;
  arrayof(AREQ) requestsArray = NULL;
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

  HybridScoringContext combineCtx = {
    .scoringType = HYBRID_SCORING_RRF,
    .rrfCtx = {
      .k = 1,
      .window = 20
    }
  };

  // Look for optional COMBINE parameter
  if (AC_AdvanceIfMatch(&ac, "COMBINE")) {
    if (parseCombine(&ac, &combineCtx, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // Save the current position to determine remaining arguments for the merge part
  const int remainingOffset = (int) ac.offset;
  const int remainingArgs = argc - 2 - remainingOffset;

  AGGPlan* mergePlan = NULL;
  // If there are remaining arguments, compile them into the request
  if (remainingArgs > 0) {
    mergeAreq = AREQ_New();
    if (AREQ_Compile(mergeAreq, argv + 2 + remainingOffset, remainingArgs, status) != REDISMODULE_OK) {
      goto error;
    }
    mergePlan = &mergeAreq->ap;

    mergeAreq->protocol = is_resp3(ctx) ? 3 : 2;

    searchRequest->searchopts.params = Param_DictClone(mergeAreq->searchopts.params);
    vectorRequest->searchopts.params = Param_DictClone(mergeAreq->searchopts.params);
  }

  // Apply context to the requests as part of parsing
  if (AREQ_ApplyContext(searchRequest, sctx, status) != REDISMODULE_OK) {
    goto error;
  }

  if (AREQ_ApplyContext(vectorRequest, sctx, status) != REDISMODULE_OK) {
    goto error;
  }

  // Create the hybrid request with proper structure
  requestsArray = array_new(AREQ, 2);
  array_append(requestsArray, *searchRequest);
  array_append(requestsArray, *vectorRequest);

  AggregationPipeline mergePipeline = {0};
  if (mergePlan) {
    mergePipeline.ap = *mergePlan;
  }
  mergePipeline.sctx = sctx;

  HybridRequest *hybridRequest = rm_malloc(sizeof(HybridRequest));
  *hybridRequest = (HybridRequest){requestsArray, 2, mergePipeline, combineCtx};

  if (mergeAreq) {
    AREQ_Free(mergeAreq);
  }

  return hybridRequest;

error:
  AREQ_Free(searchRequest);
  AREQ_Free(vectorRequest);
  if (mergeAreq) {
    AREQ_Free(mergeAreq);
  }
  array_free(requestsArray);

  if (combineCtx.scoringType == HYBRID_SCORING_LINEAR && combineCtx.linearCtx.linearWeights) {
    rm_free(combineCtx.linearCtx.linearWeights);
  }

  return NULL;
}

void HybridRequest_Free(HybridRequest *hybridRequest) {
  if (!hybridRequest) return;

  // Free the merge pipeline's result processors
  QueryProcessingCtx *qctx = &hybridRequest->merge.qctx;
  ResultProcessor *rp = qctx->endProc;
  while (rp) {
    ResultProcessor *next = rp->upstream;
    rp->Free(rp);
    rp = next;
  }

  // Free the merge pipeline's AGGPlan steps
  AGPLN_FreeSteps(&hybridRequest->merge.ap);

  // Free the merge pipeline's output fields
  FieldList_Free(&hybridRequest->merge.outFields);

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

  // Parse the hybrid request parameters (includes context application)
  HybridRequest *hybridRequest = parseHybridRequest(ctx, argv, argc, sctx, &status);
  if (!hybridRequest) {
    goto error;
  }

  // TODO: Add build pipeline and execute command here

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
