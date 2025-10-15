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
#include "hybrid/parse/hybrid_optional_args.h"

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
    {.name = "YIELD_SCORE_AS", .type = AC_ARGTYPE_STRING, .target = &searchOpts->scoreAlias},
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

    // AC_ERR_ENOENT - check if it's VSIM or just an unknown argument (special error message for DIALECT)
    const char *cur;
    rv = AC_GetString(ac, &cur, NULL, AC_F_NOADVANCE);

    if (rv == AC_OK && !strcasecmp("VSIM", cur)) {
      // Hit VSIM, we're done with search options
      break;
    }
    if (rv == AC_OK && !strcasecmp("DIALECT", cur)) {
      QueryError_SetError(status, QUERY_EPARSEARGS, DIALECT_ERROR_MSG);
      return REDISMODULE_ERR;
    }

    // Unknown argument that's not VSIM - this is an error
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown argument", " `%s` in SEARCH", cur);
    return REDISMODULE_ERR;
  }
  if (searchOpts->scoreAlias) {
    AREQ_AddRequestFlags(sreq, QEXEC_F_SEND_SCORES_AS_FIELD);
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
        QueryError_SetError(status, QUERY_EBADVAL, "Invalid YIELD_SCORE_AS value");
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
        QueryError_SetError(status, QUERY_EBADVAL, "Invalid YIELD_SCORE_AS value");
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

  if (AC_AdvanceIfMatch(ac, "DIALECT")) {
    QueryError_SetError(status, QUERY_EPARSEARGS, DIALECT_ERROR_MSG);
    goto error;
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

// Copy request configuration from source to destination
static void copyRequestConfig(RequestConfig *dest, const RequestConfig *src) {
  dest->queryTimeoutMS = src->queryTimeoutMS;
  dest->dialectVersion = src->dialectVersion;
  dest->timeoutPolicy = src->timeoutPolicy;
  dest->printProfileClock = src->printProfileClock;
  dest->BM25STD_TanhFactor = src->BM25STD_TanhFactor;
}

static void copyCursorConfig(CursorConfig *dest, const CursorConfig *src) {
  dest->maxIdle = src->maxIdle;
  dest->chunkSize = src->chunkSize;
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
  // Apply K = min(K, WINDOW)  prevents wasteful computation
  if (pvd && pvd->query->type == VECSIM_QT_KNN) {
    size_t windowValue;
    if (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
      windowValue = hybridParams->scoringCtx->rrfCtx.window;
    } else { // (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_LINEAR) {
      windowValue = hybridParams->scoringCtx->linearCtx.window;
    }
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

// This cannot be easily merged with IsIndexCoherent from aggregate_request.c since aggregate request parses prefixes differently.
// Unifying would require some refactor on the aggregate flow.
static bool IsIndexCoherentWithQuery(arrayof(const char*) prefixes, IndexSpec *spec)  {

  size_t n_prefixes = array_len(prefixes);
  if (n_prefixes == 0) {
    // No prefixes in the query --> No validation needed.
    return true;
  }

  if (n_prefixes > 0 && (!spec || !spec->rule || !spec->rule->prefixes)) {
    // Index has no prefixes, but query has prefixes --> Incoherent
    return false;
  }

  arrayof(HiddenUnicodeString*) spec_prefixes = spec->rule->prefixes;
  if (n_prefixes != array_len(spec_prefixes)) {
    return false;
  }

  // Validate that the prefixes in the arguments are the same as the ones in the
  // index (also in the same order)
  // The prefixes start right after the number
  for (uint i = 0; i < n_prefixes; i++) {
    if (HiddenUnicodeString_CompareC(spec_prefixes[i], prefixes[i]) != 0) {
      // Unmatching prefixes
      return false;
    }
  }

  return true;
}

/**
 * Parse FT.HYBRID command arguments and build a complete HybridRequest structure.
 *
 * Expected format: FT.HYBRID <index> SEARCH <query> [SCORER <scorer>] VSIM <vector_args>
 *                  [COMBINE <method> [params]] [aggregation_options]
 *
 * @param ctx Redis module context
 * @param ac ArgsCursor for parsing command arguments - should start after the index name
 * @param sctx Search context for the index (takes ownership)
 * @param parsedCmdCtx Parsed command context containing AREQs and pipeline parameters
 * @param status Output parameter for error reporting
 * @param internal Whether the request is internal (not exposed to the user)
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int parseHybridCommand(RedisModuleCtx *ctx, ArgsCursor *ac,
                       RedisSearchCtx *sctx, ParseHybridCommandCtx *parsedCmdCtx,
                       QueryError *status, bool internal) {
  HybridPipelineParams *hybridParams = parsedCmdCtx->hybridParams;
  hybridParams->scoringCtx = HybridScoringContext_NewDefault();

  // Individual variables used for parsing the tail of the command
  uint32_t *mergeReqflags = &hybridParams->aggregationParams.common.reqflags;
  // Don't expect any flag to be on yet
  RS_ASSERT(*mergeReqflags == 0);
  *parsedCmdCtx->reqConfig = RSGlobalConfig.requestConfigParams;

  // Use default dialect if > 1, otherwise use dialect 2
  if (parsedCmdCtx->reqConfig->dialectVersion < MIN_HYBRID_DIALECT) {
    parsedCmdCtx->reqConfig->dialectVersion = MIN_HYBRID_DIALECT;
  }
  parsedCmdCtx->search->reqConfig.dialectVersion = parsedCmdCtx->reqConfig->dialectVersion;
  parsedCmdCtx->vector->reqConfig.dialectVersion = parsedCmdCtx->reqConfig->dialectVersion;

  RSSearchOptions mergeSearchopts = {0};
  RSSearchOptions_Init(&mergeSearchopts);
  size_t maxHybridResults = RSGlobalConfig.maxSearchResults;

  AREQ *vectorRequest = parsedCmdCtx->vector;
  AREQ *searchRequest = parsedCmdCtx->search;

  searchRequest->reqflags |= QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY;
  vectorRequest->reqflags |= QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY;

  searchRequest->ast.validationFlags |= QAST_NO_VECTOR;
  vectorRequest->ast.validationFlags |= QAST_NO_WEIGHT | QAST_NO_VECTOR;

  // Prefixes for the index
  arrayof(const char*) prefixes = array_new(const char*, 0);

  if (AC_IsAtEnd(ac) || !AC_AdvanceIfMatch(ac, "SEARCH")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "SEARCH argument is required");
    goto error;
  }

  if (parseSearchSubquery(ac, searchRequest, status) != REDISMODULE_OK) {
    goto error;
  }

  if (parseVectorSubquery(ac, vectorRequest, status) != REDISMODULE_OK) {
    goto error;
  }

  HybridParseContext hybridParseCtx = {
      .status = status,
      .specifiedArgs = 0,
      .hybridScoringCtx = hybridParams->scoringCtx,
      .numSubqueries = HYBRID_REQUEST_NUM_SUBQUERIES,
      .plan = parsedCmdCtx->tailPlan,
      .reqFlags = mergeReqflags,
      .searchopts = &mergeSearchopts,
      .cursorConfig = parsedCmdCtx->cursorConfig,
      .reqConfig = parsedCmdCtx->reqConfig,
      .maxResults = &maxHybridResults,
      .prefixes = &prefixes,
  };
  // may change prefixes in internal array_ensure_append_1
  if (HybridParseOptionalArgs(&hybridParseCtx, ac, internal) != REDISMODULE_OK) {
    goto error;
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

  const bool hadArgumentBesidesCombine = (hybridParseCtx.specifiedArgs & ~SPECIFIED_ARG_COMBINE) != 0;
  if (hadArgumentBesidesCombine) {
    *mergeReqflags |= QEXEC_F_IS_HYBRID_TAIL;
    if (mergeSearchopts.params) {
      searchRequest->searchopts.params = Param_DictClone(mergeSearchopts.params);
      vectorRequest->searchopts.params = Param_DictClone(mergeSearchopts.params);
      Param_DictFree(mergeSearchopts.params);
      mergeSearchopts.params = NULL;
    }

    if (*mergeReqflags & QEXEC_F_IS_CURSOR) {
      // We need to turn on the cursor flag so the cursor id will be sent back when reading from the cursor
      searchRequest->reqflags |= QEXEC_F_IS_CURSOR;
      vectorRequest->reqflags |= QEXEC_F_IS_CURSOR;
      // Copy cursor configuration using the helper function
      copyCursorConfig(&searchRequest->cursorConfig, parsedCmdCtx->cursorConfig);
      copyCursorConfig(&vectorRequest->cursorConfig, parsedCmdCtx->cursorConfig);
    }
    if (*mergeReqflags & QEXEC_F_SEND_SCORES) {
      searchRequest->reqflags |= QEXEC_F_SEND_SCORES;
      vectorRequest->reqflags |= QEXEC_F_SEND_SCORES;
    }

    // Copy request configuration using the helper function
    copyRequestConfig(&searchRequest->reqConfig, parsedCmdCtx->reqConfig);
    copyRequestConfig(&vectorRequest->reqConfig, parsedCmdCtx->reqConfig);

    // Copy max results limits
    searchRequest->maxSearchResults = maxHybridResults;
    searchRequest->maxAggregateResults = maxHybridResults;
    vectorRequest->maxSearchResults = maxHybridResults;
    vectorRequest->maxAggregateResults = maxHybridResults;

    if (QAST_EvalParams(&vectorRequest->ast, &vectorRequest->searchopts, 2, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  // In the search subquery we want the sorter result processor to be in the upstream of the loader
  // This is because the sorter limits the number of results and can reduce the amount of work the loader needs to do
  // So it is important this is done before we add the load step to the subqueries plan
  PLN_ArrangeStep *arrangeStep = AGPLN_GetOrCreateArrangeStep(&parsedCmdCtx->search->pipeline.ap);
  if (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
    arrangeStep->limit = hybridParams->scoringCtx->rrfCtx.window;
  } else {
    // hybridParams->scoringCtx->scoringType == HYBRID_SCORING_LINEAR
    arrangeStep->limit = hybridParams->scoringCtx->linearCtx.window;
  }

  // We need a load step, implicit or an explicit one
  PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(parsedCmdCtx->tailPlan, NULL, NULL, PLN_T_LOAD);
  if (!loadStep) {
    // TBH don't think we need this implicit step, added to somehow affect the resulting response format
    // We wanted that by default the key and score would be returned to the user
    // This should probably be done in the hybrid send chunk where we decide on the response format.
    // For now keeping it as is - due to time constraints
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

  if (!IsIndexCoherentWithQuery(*hybridParseCtx.prefixes, parsedCmdCtx->search->sctx->spec)) {
    QueryError_SetError(status, QUERY_EMISSMATCH, NULL);
    goto error;
  }
  array_free(prefixes);
  prefixes = NULL;

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
              .scoreAlias = mergeSearchopts.scoreAlias,
          },
      .outFields = NULL,
      .maxResultsLimit = maxHybridResults,
      .language = searchRequest->searchopts.language,
  };

  hybridParams->aggregationParams = params;
  hybridParams->synchronize_read_locks = true;

  return REDISMODULE_OK;

error:
  array_free(prefixes);
  prefixes = NULL;
  if (mergeSearchopts.params) {
    Param_DictFree(mergeSearchopts.params);
  }
  if (hybridParams->scoringCtx) {
    HybridScoringContext_Free(hybridParams->scoringCtx);
    hybridParams->scoringCtx = NULL;
  }
  return REDISMODULE_ERR;
}
