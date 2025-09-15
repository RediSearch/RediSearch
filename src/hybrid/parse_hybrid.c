/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "parse_hybrid.h"
#include "query_optimizer.h"

#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "aggregate/aggregate.h"
#include "vector_query_utils.h"
#include "vector_index.h"
#include "query_error.h"
#include "spec.h"
#include "param.h"
#include "rmalloc.h"

#include "rmutil/args.h"
#include "rmutil/rm_assert.h"
#include "util/references.h"
#include "util/workers.h"
#include "info/info_redis/threads/current_thread.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"
#include "hybrid/hybrid_request.h"
#include "ext/default.h"


static VecSimRawParam createVecSimRawParam(const char *name, size_t nameLen, const char *value, size_t valueLen) {
  return (VecSimRawParam){
    .name = rm_strndup(name, nameLen),
    .nameLen = nameLen,
    .value = rm_strndup(value, valueLen),
    .valLen = valueLen
  };
}

static void addVectorQueryParam(VectorQuery *vq, const char *name, size_t nameLen, const char *value, size_t valueLen) {
  VecSimRawParam rawParam = createVecSimRawParam(name, nameLen, value, valueLen);
  vq->params.params = array_ensure_append_1(vq->params.params, rawParam);
  bool needResolve = false;
  vq->params.needResolve = array_ensure_append_1(vq->params.needResolve, needResolve);
}

static int parseSearchSubquery(ArgsCursor *ac, AREQ *sreq, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "No query string provided for SEARCH");
    return REDISMODULE_ERR;
  }

  sreq->query = AC_GetStringNC(ac, NULL);
  RSSearchOptions *searchOpts = &sreq->searchopts;

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

static int parseKNNClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector KNN ...
  //                              ^
  // Try to get number of parameters
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count");
    return REDISMODULE_ERR;
  } else if (params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid parameter count");
    return REDISMODULE_ERR;
  }

  bool hasEF = false;
  RS_ASSERT(pvd->distanceFieldAlias == NULL);

  for (int i=0; i<params; i+=2) {
    if (AC_IsAtEnd(ac)) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Missing parameter");
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "K")) {
      if (pvd->hasExplicitK) { // codespell:ignore
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate K parameter");
        return REDISMODULE_ERR;
      }
      long long kValue;
      if (AC_GetLongLong(ac, &kValue, AC_F_GE1) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value");
        return REDISMODULE_ERR;
      }
      vq->knn.k = (size_t)kValue;
      pvd->hasExplicitK = true;

    } else if (AC_AdvanceIfMatch(ac, "EF_RUNTIME")) {
      if (hasEF) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EF_RUNTIME parameter");
        return REDISMODULE_ERR;
      }
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EF_RUNTIME value");
        return REDISMODULE_ERR;
      }
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EFRUNTIME, strlen(VECSIM_EFRUNTIME), value, strlen(value));
      hasEF = true;

    } else if (AC_AdvanceIfMatch(ac, "YIELD_DISTANCE_AS")) {
      if (pvd->distanceFieldAlias != NULL) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
        return REDISMODULE_ERR;
      }
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing distance field name");
        return REDISMODULE_ERR;
      }
      // Add as QueryAttribute (for query node processing, not vector-specific)
      QueryAttribute attr = {
        .name = YIELD_DISTANCE_ATTR,
        .namelen = strlen(YIELD_DISTANCE_ATTR),
        .value = rm_strdup(value),
        .vallen = strlen(value)
      };
      pvd->attributes = array_ensure_append_1(pvd->attributes, attr);
      pvd->distanceFieldAlias = rm_strdup(value);
    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in KNN", current);
      return REDISMODULE_ERR;
    }
  }
  if (!pvd->hasExplicitK) { // codespell:ignore
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing K parameter");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseRangeClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector RANGE ...
  //                                ^
  long long params;
  if (AC_GetLongLong(ac, &params, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing parameter count");
    return REDISMODULE_ERR;
  } else if (params == 0 || params % 2 != 0) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid parameter count");
    return REDISMODULE_ERR;
  }
  bool hasRadius = false;
  bool hasEpsilon = false;
  RS_ASSERT(pvd->distanceFieldAlias == NULL);

  for (int i=0; i<params; i+=2) {
    if (AC_IsAtEnd(ac)) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Missing parameter");
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "RADIUS")) {
      if (hasRadius) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate RADIUS parameter");
        return REDISMODULE_ERR;
      }
      double radiusValue;
      if (AC_GetDouble(ac, &radiusValue, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid RADIUS value");
        return REDISMODULE_ERR;
      }
      vq->range.radius = radiusValue;
      hasRadius = true;

    } else if (AC_AdvanceIfMatch(ac, "EPSILON")) {
      if (hasEpsilon) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EPSILON parameter");
        return REDISMODULE_ERR;
      }
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EPSILON value");
        return REDISMODULE_ERR;
      }
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EPSILON, strlen(VECSIM_EPSILON), value, strlen(value));
      hasEpsilon = true;

    } else if (AC_AdvanceIfMatch(ac, "YIELD_DISTANCE_AS")) {
      if (pvd->distanceFieldAlias != NULL) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
        return REDISMODULE_ERR;
      }
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing distance field name");
        return REDISMODULE_ERR;
      }
      // Add as QueryAttribute (for query node processing, not vector-specific)
      QueryAttribute attr = {
        .name = YIELD_DISTANCE_ATTR,
        .namelen = strlen(YIELD_DISTANCE_ATTR),
        .value = rm_strdup(value),
        .vallen = strlen(value)
      };
      pvd->attributes = array_ensure_append_1(pvd->attributes, attr);
      pvd->distanceFieldAlias = rm_strdup(value);
    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
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
static int parseFilterClause(ArgsCursor *ac, AREQ *vreq, QueryError *status) {
  // VSIM @vectorfield vector [KNN/RANGE ...] FILTER ...
  //                                                 ^
  vreq->query = AC_GetStringNC(ac, NULL);
  return REDISMODULE_OK;
}

static int parseVectorSubquery(ArgsCursor *ac, AREQ *vreq, QueryError *status) {
  // Check for required VSIM keyword
  if (!AC_AdvanceIfMatch(ac, "VSIM")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "VSIM parameter is required");
    return REDISMODULE_ERR;
  }

  // Create ParsedVectorData struct at the beginning for cleaner error handling
  ParsedVectorData *pvd = rm_calloc(1, sizeof(ParsedVectorData));
  pvd->queryNodeFlags = QueryNode_YieldsDistance | QueryNode_HybridVectorSubqueryNode;

  // Allocate VectorQuery directly (params arrays will be created lazily by array_ensure_append_1)
  VectorQuery *vq = rm_calloc(1, sizeof(VectorQuery));
  pvd->query = vq;

  // Parse vector field name and store it for later resolution
  const char *fieldNameWithPrefix;
  size_t fieldNameLen;
  if (AC_GetString(ac, &fieldNameWithPrefix, &fieldNameLen, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector field name");
    goto error;
  }

  // Check if field name starts with @ prefix
  if (fieldNameLen == 0 || fieldNameWithPrefix[0] != '@') {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing @ prefix for vector field name");
    goto error;
  }

  // Skip the @ prefix and store the field name
  pvd->fieldName = fieldNameWithPrefix + 1;

  const char *vectorParam;
  size_t vectorParamLen;
  if (AC_GetString(ac, &vectorParam, &vectorParamLen, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector parameter");
    goto error;
  }

  if (vectorParam[0] == '$') {
    // PARAMETER CASE: store parameter name for later resolution
    vectorParam++;  // Skip '$'
    vectorParamLen--;  // Adjust length for skipped '$'
    pvd->isParameter = true;
  }

  // Set default KNN values before checking for more parameters
  vq->type = VECSIM_QT_KNN;
  vq->knn.k = HYBRID_DEFAULT_KNN_K;
  vq->knn.order = BY_SCORE;
  pvd->hasExplicitK = false;

  if (AC_IsAtEnd(ac)) goto final;

  // Parse optional KNN or RANGE clause
  if (AC_AdvanceIfMatch(ac, "KNN")) {
    if (parseKNNClause(ac, vq, pvd, status) != REDISMODULE_OK) {
      goto error;
    }
    vq->type = VECSIM_QT_KNN;
    vq->knn.order = BY_SCORE;
  } else if (AC_AdvanceIfMatch(ac, "RANGE")) {
    if (parseRangeClause(ac, vq, pvd, status) != REDISMODULE_OK) {
      goto error;
    }
    vq->type = VECSIM_QT_RANGE;
    vq->range.order = BY_SCORE;
  }

  // Check for optional FILTER clause - parameter may not be in our scope
  if (AC_AdvanceIfMatch(ac, "FILTER")) {
    if (parseFilterClause(ac, vreq, status) != REDISMODULE_OK) {
      goto error;
    }
  }

final:
  if (!vreq->query) {  // meaning there is no filter clause
    vreq->query = "*";
  }

  // Set vector data in VectorQuery based on type (KNN vs RANGE)
  // The type should be set by now from parseKNNClause or parseRangeClause
  switch (vq->type) {
    case VECSIM_QT_KNN:
      vq->knn.vector = (void*)vectorParam;
      vq->knn.vecLen = vectorParamLen;
      break;
    case VECSIM_QT_RANGE:
      vq->range.vector = (void*)vectorParam;
      vq->range.vecLen = vectorParamLen;
      break;
  }

  // Set default scoreField using vector field name (can be done during parsing)
  VectorQuery_SetDefaultScoreField(vq, pvd->fieldName, strlen(pvd->fieldName));

  // Store the completed ParsedVectorData in AREQ
  vreq->parsedVectorData = pvd;

  return REDISMODULE_OK;

error:
  ParsedVectorData_Free(pvd);
  return REDISMODULE_ERR;
}

/**
 * Parse COMBINE clause parameters for hybrid scoring configuration.
 *
 * Supports LINEAR (requires numWeights weight values) and RRF (optional CONSTANT and WINDOW parameters).
 * Defaults to RRF if no method specified. Uses hybrid-specific defaults: RRF CONSTANT=60, WINDOW=20.
 * WINDOW parameter controls the number of results consumed from each subquery before fusion.
 * When WINDOW is not explicitly set, it can be overridden by LIMIT parameter in fallback logic.
 *
 * @param ac Arguments cursor positioned after "COMBINE"
 * @param combineCtx Hybrid scoring context to populate (window field and hasExplicitWindow flag are set)
 * @param numWeights Number of weight values required for LINEAR scoring
 * @param status Output parameter for error reporting
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
static int parseCombine(ArgsCursor *ac, HybridScoringContext *combineCtx, size_t numWeights, QueryError *status) {
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
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;

    // Parse the weight values directly
    for (size_t i = 0; i < numWeights; i++) {
      double weight;
      if (AC_GetDouble(ac, &weight, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Missing or invalid weight value in LINEAR weights");
        goto error;
      }
      combineCtx->linearCtx.linearWeights[i] = weight;
    }
  } else if (combineCtx->scoringType == HYBRID_SCORING_RRF) {
    // For RRF, we need constant and window parameters
    ArgsCursor params = {0};
    int rv = AC_GetVarArgs(ac, &params);

    // Initialize with defaults
    combineCtx->rrfCtx.constant = HYBRID_DEFAULT_RRF_CONSTANT;
    combineCtx->rrfCtx.window = HYBRID_DEFAULT_WINDOW;
    combineCtx->rrfCtx.hasExplicitWindow = false;

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

        if (strcasecmp(paramName, "CONSTANT") == 0) {
          double constant;
          if (AC_GetDouble(&params, &constant, 0) != AC_OK || constant <= 0) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid CONSTANT value in RRF");
            goto error;
          }
          combineCtx->rrfCtx.constant = constant;
        } else if (strcasecmp(paramName, "WINDOW") == 0) {
          long long window;
          if (AC_GetLongLong(&params, &window, 0) != AC_OK || window <= 0) {
            QueryError_SetError(status, QUERY_ESYNTAX, "Invalid WINDOW value in RRF");
            goto error;
          }
          combineCtx->rrfCtx.window = window;
          combineCtx->rrfCtx.hasExplicitWindow = true;
        } else {
          QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RRF", paramName);
          goto error;
        }
      }
    }
  }

  return REDISMODULE_OK;
error:
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

// Helper function to get LIMIT value from parsed aggregation pipeline
static size_t getLimitFromPlan(AGGPlan *plan) {
  RS_ASSERT(plan);

  PLN_ArrangeStep *arrangeStep = AGPLN_GetArrangeStep(plan);
  if (arrangeStep && arrangeStep->isLimited && arrangeStep->limit > 0) {
    return (size_t)arrangeStep->limit;
  }
  return 0;
}

// Helper function to check if LIMIT was explicitly provided
static bool tailHasExplicitLimitInPlan(AGGPlan *plan) {
  if (!plan) return false;

  PLN_ArrangeStep *arrangeStep = AGPLN_GetArrangeStep(plan);
  return (arrangeStep && arrangeStep->isLimited);
}

/**
 * Apply LIMIT parameter fallback logic to KNN K and WINDOW parameters.
 *
 * When LIMIT is explicitly provided but KNN K or WINDOW are not explicitly set,
 * this function applies the LIMIT value as a fallback for those parameters instead of their
 * defaults (unless they have been explicitly set).
 * This ensures consistent behavior where LIMIT acts as a unified size hint
 * for hybrid search operations.
 *
 * @param tailPipeline The pipeline to extract LIMIT from
 * @param pvd The parsed vector data containing KNN parameters
 * @param hybridParams The hybrid parameters containing WINDOW settings
 */
static void applyLimitParameterFallbacks(AGGPlan *tailPlan,
                                       ParsedVectorData *pvd,
                                       HybridPipelineParams *hybridParams) {
  size_t limitValue = getLimitFromPlan(tailPlan);
  bool hasExplicitLimit = tailHasExplicitLimitInPlan(tailPlan);

  // Apply LIMIT → KNN K fallback ONLY if K was not explicitly set AND LIMIT was explicitly provided
  if (pvd && pvd->query->type == VECSIM_QT_KNN &&
      !pvd->hasExplicitK &&
      hasExplicitLimit && limitValue > 0) {
    pvd->query->knn.k = limitValue;
  }

  // Apply LIMIT → WINDOW fallback ONLY if WINDOW was not explicitly set AND LIMIT was explicitly provided
  if (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF &&
      !hybridParams->scoringCtx->rrfCtx.hasExplicitWindow &&
      hasExplicitLimit && limitValue > 0) {
    hybridParams->scoringCtx->rrfCtx.window = limitValue;
  }
}

/**
 * Apply KNN K ≤ WINDOW constraint for RRF scoring to prevent wasteful computation.
 *
 * The RRF merger only considers the top WINDOW results from each component,
 * so having KNN K > WINDOW would fetch unnecessary results that won't be used.
 * This constraint is applied after all parameter resolution (defaults, explicit values,
 * and LIMIT fallbacks) is complete.
 *
 * @param pvd The parsed vector data containing KNN parameters
 * @param hybridParams The hybrid parameters containing WINDOW settings
 */
static void applyKNNTopKWindowConstraint(ParsedVectorData *pvd,
                                 HybridPipelineParams *hybridParams) {
  // Apply K ≤ WINDOW constraint for RRF scoring to prevent wasteful computation
  if (pvd && pvd->query->type == VECSIM_QT_KNN &&
      hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
    size_t windowValue = hybridParams->scoringCtx->rrfCtx.window;
    if (pvd->query->knn.k > windowValue) {
      pvd->query->knn.k = windowValue;
    }
  }
}

// Field names for implicit LOAD step
#define HYBRID_IMPLICIT_KEY_FIELDS UNDERSCORE_KEY, UNDERSCORE_SCORE
#define HYBRID_IMPLICIT_KEY_FIELD_COUNT 2

/**
 * Create implicit LOAD step for document key when no explicit LOAD is specified.
 * Returns a PLN_LoadStep that loads only the HYBRID_IMPLICIT_KEY_FIELD.
 */
static PLN_LoadStep *createImplicitLoadStep(void) {
    // Use a static array for the field name - no memory management needed
    static const char *implicitArgv[] = {HYBRID_IMPLICIT_KEY_FIELDS};

    PLN_LoadStep *implicitLoadStep = rm_calloc(1, sizeof(PLN_LoadStep));

    // Set up base step properties - use standard loadDtor
    implicitLoadStep->base.type = PLN_T_LOAD;
    implicitLoadStep->base.alias = NULL;
    implicitLoadStep->base.flags = 0;
    implicitLoadStep->base.dtor = loadDtor; // Use standard destructor

    // Create ArgsCursor with static array - no memory management needed
    ArgsCursor_InitCString(&implicitLoadStep->args, implicitArgv, HYBRID_IMPLICIT_KEY_FIELD_COUNT);

    // Pre-allocate keys array for the number of fields to load
    implicitLoadStep->nkeys = 0;
    implicitLoadStep->keys = rm_calloc(implicitLoadStep->args.argc, sizeof(RLookupKey*));

    return implicitLoadStep;
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
 */
int parseHybridCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                       RedisSearchCtx *sctx, const char *indexname, ParseHybridCommandCtx *parsedCmdCtx,
                       QueryError *status) {
  HybridPipelineParams *hybridParams = parsedCmdCtx->hybridParams;
  hybridParams->scoringCtx = HybridScoringContext_NewDefault();

  // Individual variables used for parsing the tail of the command
  uint32_t *mergeReqflags = &hybridParams->aggregationParams.common.reqflags;
  // Don't expect any flag to be on yet
  RS_ASSERT(*mergeReqflags == 0);
  *parsedCmdCtx->reqConfig = RSGlobalConfig.requestConfigParams;
  RSSearchOptions mergeSearchopts = {0};
  size_t mergeMaxSearchResults = RSGlobalConfig.maxSearchResults;
  size_t mergeMaxAggregateResults = RSGlobalConfig.maxAggregateResults;

  AREQ *vectorRequest = parsedCmdCtx->vector;
  AREQ *searchRequest = parsedCmdCtx->search;

  searchRequest->reqflags |= QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY;
  vectorRequest->reqflags |= QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY;

  searchRequest->ast.validationFlags |= QAST_NO_VECTOR;
  vectorRequest->ast.validationFlags |= QAST_NO_WEIGHT | QAST_NO_VECTOR;

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
    if (parseCombine(&ac, hybridParams->scoringCtx, HYBRID_REQUEST_NUM_SUBQUERIES, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  if (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_LINEAR) {
    if (!searchRequest->searchopts.scorerName) {
      // if no explicit scorer, set the default one
      searchRequest->searchopts.scorerName = BM25_STD_NORMALIZED_MAX_SCORER_NAME;
    }
  }

  // If YIELD_DISTANCE_AS was specified, use its string (pass ownership from pvd to vnStep),
  // otherwise, store the distance in a default key.
  const char *distanceFieldAlias = NULL;
  if (vectorRequest->parsedVectorData->distanceFieldAlias != NULL) {
    distanceFieldAlias = vectorRequest->parsedVectorData->distanceFieldAlias;
    vectorRequest->parsedVectorData->distanceFieldAlias = NULL;
  } else {
    distanceFieldAlias = VectorQuery_GetDefaultScoreFieldName(
      vectorRequest->parsedVectorData->fieldName,
      strlen(vectorRequest->parsedVectorData->fieldName)
    );
    vectorRequest->parsedVectorData->queryNodeFlags |= QueryNode_HideVectorDistanceField;
  }
  // Store the key string so it could fetch the distance from the RlookupRow
  PLN_VectorNormalizerStep *vnStep = PLNVectorNormalizerStep_New(
    vectorRequest->parsedVectorData->fieldName,
    distanceFieldAlias
  );
  AGPLN_AddStep(&vectorRequest->pipeline.ap, &vnStep->base);

  // Save the current position to determine remaining arguments for the merge part
  const int remainingOffset = (int) ac.offset;
  const int remainingArgs = argc - 2 - remainingOffset;

  // If there are remaining arguments, parse them into the aggregate plan
  if (remainingArgs > 0) {
    *mergeReqflags |= QEXEC_F_IS_HYBRID_TAIL;
    RSSearchOptions_Init(&mergeSearchopts);

    ParseAggPlanContext papCtx = {
      .plan = parsedCmdCtx->tailPlan,
      .reqflags = mergeReqflags,
      .reqConfig = parsedCmdCtx->reqConfig,
      .searchopts = &mergeSearchopts,
      .prefixesOffset = NULL,               // Invalid in FT.HYBRID
      .cursorConfig = parsedCmdCtx->cursorConfig,
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
    copyRequestConfig(&searchRequest->reqConfig, parsedCmdCtx->reqConfig);
    copyRequestConfig(&vectorRequest->reqConfig, parsedCmdCtx->reqConfig);

    // Copy max results limits
    searchRequest->maxSearchResults = mergeMaxSearchResults;
    searchRequest->maxAggregateResults = mergeMaxAggregateResults;
    vectorRequest->maxSearchResults = mergeMaxSearchResults;
    vectorRequest->maxAggregateResults = mergeMaxAggregateResults;

    if (QAST_EvalParams(&vectorRequest->ast, &vectorRequest->searchopts, 2, status) != REDISMODULE_OK) {
      goto error;
    }

    applyLimitParameterFallbacks(parsedCmdCtx->tailPlan, vectorRequest->parsedVectorData, hybridParams);
  }

  // In the search subquery we want the sorter result processor to be in the upstream of the loader
  // This is because the sorter limits the number of results and can reduce the amount of work the loader needs to do
  // So it is important this is done before we add the load step to the subqueries plan
  AGPLN_GetOrCreateArrangeStep(&parsedCmdCtx->search->pipeline.ap);

  // We need a load step, implicit or an explicit one
  PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(parsedCmdCtx->tailPlan, NULL, NULL, PLN_T_LOAD);
  if (!loadStep) {
    loadStep = createImplicitLoadStep();
  } else {
    AGPLN_PopStep(&loadStep->base);
  }
  AGPLN_AddStep(&searchRequest->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
  AGPLN_AddStep(&vectorRequest->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
  // Free the source load step
  loadStep->base.dtor(&loadStep->base);
  loadStep = NULL;

  if (!(*mergeReqflags & QEXEC_F_NO_SORT)) {
    // No SORTBY 0 - add implicit sort-by-score
    AGPLN_GetOrCreateArrangeStep(parsedCmdCtx->tailPlan);
  }


  // Apply KNN K ≤ WINDOW constraint after all parameter resolution is complete
  applyKNNTopKWindowConstraint(vectorRequest->parsedVectorData, hybridParams);

  // Apply context to each request
  if (AREQ_ApplyContext(searchRequest, searchRequest->sctx, status) != REDISMODULE_OK) {
    AddValidationErrorContext(searchRequest, status);
    goto error;
  }
  if (AREQ_ApplyContext(vectorRequest, vectorRequest->sctx, status) != REDISMODULE_OK) {
    AddValidationErrorContext(vectorRequest, status);
    goto error;
  }

  // thread safe context
  const AggregationPipelineParams params = {
      .common =
          {
              .sctx = sctx,  // should be a separate context?
              .reqflags = *mergeReqflags | QEXEC_F_IS_HYBRID_TAIL,
              .optimizer = NULL,  // is it?
          },
      .outFields = NULL,
      .maxResultsLimit = mergeMaxAggregateResults,
      .language = searchRequest->searchopts.language,
  };

  hybridParams->aggregationParams = params;
  hybridParams->synchronize_read_locks = true;

  return REDISMODULE_OK;

error:
  if (hybridParams->scoringCtx) {
    HybridScoringContext_Free(hybridParams->scoringCtx);
    hybridParams->scoringCtx = NULL;
  }
  return REDISMODULE_ERR;
}