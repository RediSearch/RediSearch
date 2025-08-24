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

// Helper function to set error message with proper plural vs singular form
static void setExpectedArgumentsError(QueryError *status, unsigned int expected, int provided) {
  const char *verb = (provided == 1) ? "was" : "were";
  QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Expected arguments", " %u, but %d %s provided", expected, provided, verb);
}

// Check if we're at the end of arguments in the middle of a clause and set appropriate error for missing argument
static int inline CheckEnd(ArgsCursor *ac, const char *argument, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS,
                                    "Missing argument value for ", argument);
      return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

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
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in SEARCH", cur);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static int parseKNNClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector KNN ...
  //                              ^
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing argument count");
    return REDISMODULE_ERR;
  }
  // Try to get number of arguments
  unsigned int argumentCount;
  if (AC_GetUnsigned(ac, &argumentCount, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid argument count: expected an unsigned integer");
    return REDISMODULE_ERR;
  } else if (argumentCount == 0 || argumentCount % 2 != 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Invalid argument count", ": %u (must be a positive even number for key/value pairs)", argumentCount);
    return REDISMODULE_ERR;
  }

  bool hasEF = false;
  RS_ASSERT(pvd->vectorScoreFieldAlias == NULL);

  for (int i=0; i<argumentCount; i+=2) {
    if (AC_IsAtEnd(ac)) {
      setExpectedArgumentsError(status, argumentCount, i);
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "K")) {
      if (pvd->hasExplicitK) { // codespell:ignore
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate K argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "K", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      long long kValue;
      if (AC_GetLongLong(ac, &kValue, AC_F_GE1) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value");
        return REDISMODULE_ERR;
      }
      vq->knn.k = (size_t)kValue;
      pvd->hasExplicitK = true;

    } else if (AC_AdvanceIfMatch(ac, "EF_RUNTIME")) {
      if (hasEF) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EF_RUNTIME argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "EF_RUNTIME", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EF_RUNTIME value");
        return REDISMODULE_ERR;
      }
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EFRUNTIME, strlen(VECSIM_EFRUNTIME), value, strlen(value));
      hasEF = true;

    } else if (AC_AdvanceIfMatch(ac, "YIELD_SCORE_AS")) {
      if (pvd->vectorScoreFieldAlias != NULL) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_SCORE_AS argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "YIELD_SCORE_AS", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_EBADVAL, "Invalid vector score field name");
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
      pvd->vectorScoreFieldAlias = rm_strdup(value);
    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in KNN", current);
      return REDISMODULE_ERR;
    }
  }
  if (!pvd->hasExplicitK) { // codespell:ignore
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing required argument K");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseRangeClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector RANGE ...
  //                                ^
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing argument count");
    return REDISMODULE_ERR;
  }
  // Try to get number of arguments
  unsigned int argumentCount;
  if (AC_GetUnsigned(ac, &argumentCount, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid argument count: expected an unsigned integer");
    return REDISMODULE_ERR;
  } else if (argumentCount == 0 || argumentCount % 2 != 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Invalid argument count", ": %u (must be a positive even number for key/value pairs)", argumentCount);
    return REDISMODULE_ERR;
  }

  bool hasRadius = false;
  bool hasEpsilon = false;
  RS_ASSERT(pvd->vectorScoreFieldAlias == NULL);

  for (int i=0; i<argumentCount; i+=2) {
    if (AC_IsAtEnd(ac)) {
      setExpectedArgumentsError(status, argumentCount, i);
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "RADIUS")) {
      if (hasRadius) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate RADIUS argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "RADIUS", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      double radiusValue;
      if (AC_GetDouble(ac, &radiusValue, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid RADIUS value");
        return REDISMODULE_ERR;
      }
      vq->range.radius = radiusValue;
      hasRadius = true;

    } else if (AC_AdvanceIfMatch(ac, "EPSILON")) {
      if (hasEpsilon) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate EPSILON argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "EPSILON", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid EPSILON value");
        return REDISMODULE_ERR;
      }
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EPSILON, strlen(VECSIM_EPSILON), value, strlen(value));
      hasEpsilon = true;

    } else if (AC_AdvanceIfMatch(ac, "YIELD_SCORE_AS")) {
      if (pvd->vectorScoreFieldAlias != NULL) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate YIELD_SCORE_AS argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "YIELD_SCORE_AS", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      const char *value;
      if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_EBADVAL, "Invalid vector score field name");
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
      pvd->vectorScoreFieldAlias = rm_strdup(value);
    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in RANGE", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasRadius) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing required argument RADIUS");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseLinearClause(ArgsCursor *ac, HybridLinearContext *linearCtx, QueryError *status) {
  // LINEAR 4 ALPHA 0.1 BETA 0.9 ...
  //        ^
  unsigned int argumentCount;
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing argument count");
    return REDISMODULE_ERR;
  }
  if (AC_GetUnsigned(ac, &argumentCount, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid argument count: expected an unsigned integer");
    return REDISMODULE_ERR;
  } else if (argumentCount == 0 || argumentCount % 2 != 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Invalid argument count", ": %u (must be a positive even number for key/value pairs)", argumentCount);
    return REDISMODULE_ERR;
  }

  bool hasAlpha = false;
  bool hasBeta = false;

  for (int i=0; i<argumentCount; i+=2) {
    if (AC_IsAtEnd(ac)) {
      setExpectedArgumentsError(status, argumentCount, i);
      return REDISMODULE_ERR;
    }
    if (AC_AdvanceIfMatch(ac, "ALPHA")) {
      if (hasAlpha) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate ALPHA argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "ALPHA", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      double alphaValue;
      if (AC_GetDouble(ac, &alphaValue, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid ALPHA value");
        return REDISMODULE_ERR;
      }
      linearCtx->linearWeights[0] = alphaValue;
      hasAlpha = true;

    } else if (AC_AdvanceIfMatch(ac, "BETA")) {
      if (hasBeta) {
        QueryError_SetError(status, QUERY_EDUPPARAM, "Duplicate BETA argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "BETA", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      double betaValue;
      if (AC_GetDouble(ac, &betaValue, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid BETA value");
        return REDISMODULE_ERR;
      }
      linearCtx->linearWeights[1] = betaValue;
      hasBeta = true;

    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in LINEAR", current);
      return REDISMODULE_ERR;
    }
  }

  if (!(hasAlpha && hasBeta)) {
    bool bothMissing = !hasAlpha && !hasBeta;
    const char *missingArgs = bothMissing ? "ALPHA, BETA" : !hasAlpha ? "ALPHA" : "BETA";
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Missing value for ", missingArgs);
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
    QueryError_SetError(status, QUERY_ESYNTAX, "VSIM argument is required");
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
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing vector argument");
    goto error;
  }

  if (vectorParam[0] == '$') {
    // PARAMETER CASE: store parameter name for later resolution
    vectorParam++;  // Skip '$'
    vectorParamLen--;  // Adjust length for skipped '$'
    pvd->isParameter = true;
  }

  // Set default KNN values before checking for more arguments
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

  // Check for optional FILTER clause - argument may not be in our scope
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
 * Parse COMBINE clause arguments for hybrid scoring configuration.
 *
 * Supports LINEAR (requires numWeights weight values) and RRF (optional CONSTANT and WINDOW arguments).
 * Defaults to RRF if no method specified. Uses hybrid-specific defaults: RRF CONSTANT=60, WINDOW=20.
 * WINDOW argument controls the number of results consumed from each subquery before fusion.
 * When WINDOW is not explicitly set, it can be overridden by LIMIT argument in fallback logic.
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

  // Parse arguments based on scoring type
  if (combineCtx->scoringType == HYBRID_SCORING_LINEAR) {
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;

    if (parseLinearClause(ac, &combineCtx->linearCtx, status) != REDISMODULE_OK) {
      goto error;
    }
  } else if (combineCtx->scoringType == HYBRID_SCORING_RRF) {
    // For RRF, we need constant and window arguments
    ArgsCursor params = {0};
    int rv = AC_GetVarArgs(ac, &params);

    // Initialize with defaults
    combineCtx->rrfCtx.constant = HYBRID_DEFAULT_RRF_CONSTANT;
    combineCtx->rrfCtx.window = HYBRID_DEFAULT_WINDOW;
    combineCtx->rrfCtx.hasExplicitWindow = false;

    if (rv == AC_OK) {
      // Parameters were provided
      if (params.argc % 2 != 0) {
        QueryError_SetError(status, QUERY_ESYNTAX, "RRF arguments must be in name-value pairs");
        goto error;
      }

      // Parse the specified arguments
      while (!AC_IsAtEnd(&params)) {
        const char *paramName = AC_GetStringNC(&params, NULL);
        if (!paramName) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Missing argument name in RRF");
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
          QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in RRF", paramName);
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
  RS_ASSERT(pipeline);

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
 * Apply LIMIT argument fallback logic to KNN K and WINDOW arguments.
 *
 * When LIMIT is explicitly provided but KNN K or WINDOW are not explicitly set,
 * this function applies the LIMIT value as a fallback for those arguments instead of their
 * defaults (unless they have been explicitly set).
 * This ensures consistent behavior where LIMIT acts as a unified size hint
 * for hybrid search operations.
 *
 * @param tailPipeline The pipeline to extract LIMIT from
 * @param pvd The parsed vector data containing KNN arguments
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
 * This constraint is applied after all argument resolution (defaults, explicit values,
 * and LIMIT fallbacks) is complete.
 *
 * @param pvd The parsed vector data containing KNN arguments
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

HybridRequest *MakeDefaultHyabridRequest() {
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  arrayof(AREQ*) requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  requests = array_ensure_append_1(requests, search);
  requests = array_ensure_append_1(requests, vector);
  return HybridRequest_New(requests, array_len(requests));
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

  /*foosome initializeAREQ(searchRequest);
  initializeAREQ(vectorRequest);
  searchRequest->sctx = createDetachedSearchContext(ctx, indexname);
  vectorRequest->sctx = createDetachedSearchContext(ctx, indexname);*/

  HybridPipelineParams *hybridParams = rm_calloc(1, sizeof(HybridPipelineParams));
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
    QueryError_SetError(status, QUERY_ESYNTAX, "SEARCH argument is required");
    goto error;
  }

  if (parseSearchSubquery(&ac, parsedCmdCtx->search, status) != REDISMODULE_OK) {
    goto error;
  }

  if (parseVectorSubquery(&ac, parsedCmdCtx->vector, status) != REDISMODULE_OK) {
    goto error;
  }

  // Look for optional COMBINE argument
  if (AC_AdvanceIfMatch(&ac, "COMBINE")) {
    if (parseCombine(&ac, hybridParams->scoringCtx, HYBRID_REQUEST_NUM_SUBQUERIES, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // If YIELD_SCORE_AS was specified, use its string (pass ownership from pvd to vnStep),
  // otherwise, store the vector score in a default key.
  const char *vectorScoreFieldAlias = NULL;
  if (vectorRequest->parsedVectorData->vectorScoreFieldAlias != NULL) {
    vectorScoreFieldAlias = vectorRequest->parsedVectorData->vectorScoreFieldAlias;
    vectorRequest->parsedVectorData->vectorScoreFieldAlias = NULL;
  } else {
    vectorScoreFieldAlias = VectorQuery_GetDefaultScoreFieldName(
      vectorRequest->parsedVectorData->fieldName,
      strlen(vectorRequest->parsedVectorData->fieldName)
    );
    vectorRequest->parsedVectorData->queryNodeFlags |= QueryNode_HideVectorDistanceField;
  }
  // Store the key string so it could fetch the distance from the RlookupRow
  PLN_VectorNormalizerStep *vnStep = PLNVectorNormalizerStep_New(
    vectorRequest->parsedVectorData->fieldName,
    vectorScoreFieldAlias
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
      .reqConfig = &mergeReqConfig,
      .searchopts = &mergeSearchopts,
      .prefixesOffset = NULL,               // Invalid in FT.HYBRID
      .cursorConfig = &parsedCmdCtx->cursorConfig,
      .requiredFields = NULL,               // Invalid in FT.HYBRID
      .maxSearchResults = &mergeMaxSearchResults,
      .maxAggregateResults = &mergeMaxAggregateResults
    };
    if (parseAggPlan(&papCtx, &ac, status) != REDISMODULE_OK) {
      goto error;
    }

    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(parsedCmdCtx->tailPlan, NULL, NULL, PLN_T_LOAD);
    if (!loadStep) {
      loadStep = createImplicitLoadStep();
    } else {
      AGPLN_PopStep(&loadStep->base);
    }

    AGPLN_AddStep(&parsedCmdCtx->search->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
    AGPLN_AddStep(&parsedCmdCtx->vector->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
    // Free the source load step
    loadStep->base.dtor(&loadStep->base);
    loadStep = NULL;

    if (mergeSearchopts.params) {
      parsedCmdCtx->search->searchopts.params = Param_DictClone(mergeSearchopts.params);
      parsedCmdCtx->vector->searchopts.params = Param_DictClone(mergeSearchopts.params);
      Param_DictFree(mergeSearchopts.params);
    }

    // Copy request configuration using the helper function
    copyRequestConfig(&parsedCmdCtx->search->reqConfig, &mergeReqConfig);
    copyRequestConfig(&parsedCmdCtx->vector->reqConfig, &mergeReqConfig);

    // Copy max results limits
    parsedCmdCtx->search->maxSearchResults = mergeMaxSearchResults;
    parsedCmdCtx->search->maxAggregateResults = mergeMaxAggregateResults;
    parsedCmdCtx->vector->maxSearchResults = mergeMaxSearchResults;
    parsedCmdCtx->vector->maxAggregateResults = mergeMaxAggregateResults;

    if (QAST_EvalParams(&parsedCmdCtx->vector->ast, &parsedCmdCtx->vector->searchopts, 2, status) != REDISMODULE_OK) {
      goto error;
    }

    applyLimitParameterFallbacks(parsedCmdCtx->tailPlan, parsedCmdCtx->vector->parsedVectorData, hybridParams);
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


  // Apply KNN K ≤ WINDOW constraint after all argument resolution is complete
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
      .language = parsedCmdCtx->search->searchopts.language,
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

#define SEARCH_INDEX 0
#define VECTOR_INDEX 1

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool internal, bool coordinator) {
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

  HybridRequest *hybridRequest = MakeDefaultHyabridRequest();
  ParseHybridCommandCtx cmd = {0};
  cmd.search = hybridRequest->requests[SEARCH_INDEX];
  cmd.vector = hybridRequest->requests[VECTOR_INDEX];
  cmd.tailPlan = &hybridRequest->tailPipeline->ap;
  RedisModuleCtx *ctx1 = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(ctx1, RedisModule_GetSelectedDb(ctx));
  cmd.search->sctx = NewSearchCtxC(ctx1, indexname, true);
  RedisModuleCtx *ctx2 = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(ctx2, RedisModule_GetSelectedDb(ctx));
  cmd.vector->sctx = NewSearchCtxC(ctx2, indexname, true);

  int rc = parseHybridCommand(ctx, argv, argc, sctx, indexname, &cmd, &status);
  if (rc != REDISMODULE_OK) {
    goto error;
  }

  bool isCursor = cmd.hybridParams.aggregationParams.common.reqflags & QEXEC_F_IS_CURSOR;
  arrayof(ResultProcessor*) depleters = NULL;
  // Internal commands do not have a hybrid merger and only have a depletion pipeline
  if (internal) {
    RS_LOG_ASSERT(isCursor, "Internal hybrid command must be a cursor request from a coordinator");
    isCursor = true;
    depleters = HybridRequest_BuildDepletionPipeline(hybridRequest, &cmd.hybridParams);
    if (!depleters) {
      goto error;
    }
  } else {
    if (HybridRequest_BuildPipeline(hybridRequest, &cmd.hybridParams) != REDISMODULE_OK) {
      goto error;
    }
  }

  if (isCursor) {
    arrayof(Cursor*) cursors = HybridRequest_StartCursor(hybridRequest, depleters, coordinator);
    if (!cursors) {
      goto error;
    }

    // Send array of cursor IDs as response
    RedisModule_ReplyWithArray(ctx, array_len(cursors));
    for (size_t i = 0; i < array_len(cursors); i++) {
      RedisModule_ReplyWithLongLong(ctx, cursors[i]->id);
    }
    array_free(cursors);
  } else {
    // TODO: Add execute command here
  }
  
  StrongRef_Release(spec_ref);
  return REDISMODULE_OK;

error:
  RS_LOG_ASSERT(QueryError_HasError(&status), "Hybrid query parsing error");

  if (cmd.search) {
    if (cmd.search->sctx) {
      RedisModuleCtx *thctx = cmd.search->sctx->redisCtx;
      SearchCtx_Free(cmd.search->sctx);
      if (thctx) {
        RedisModule_FreeThreadSafeContext(thctx);
      }
      cmd.search->sctx = NULL;
    }
    AREQ_Free(cmd.search);
  }

  if (cmd.vector) {
    if (cmd.vector->sctx) {
      RedisModuleCtx *thctx = cmd.vector->sctx->redisCtx;
      SearchCtx_Free(cmd.vector->sctx);
      if (thctx) {
        RedisModule_FreeThreadSafeContext(thctx);
      }
      cmd.vector->sctx = NULL;
    }
    AREQ_Free(cmd.vector);
  }

  if (hybridRequest) {
    HybridRequest_Free(hybridRequest);
  }

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
