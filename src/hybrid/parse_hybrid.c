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
#include <stdint.h>

#include "aggregate/aggregate.h"
#include "vector_query_utils.h"
#include "vector_index.h"
#include "query_error_ffi.h"
#include "spec.h"
#include "param.h"
#include "rmalloc.h"
#include "config.h"
#include "shard_window_ratio.h"
#include "rmutil/args.h"
#include "rmutil/rm_assert.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_search_result.h"
#include "hybrid/parse/hybrid_optional_args.h"
#include "asm_state_machine.h"
#include "hybrid/parse/hybrid_callbacks.h"
#include "util/arg_parser.h"
#include "vecsim/query_results.h"
#include "vecsim/vec_sim_common.h"
#include "document.h"
#include "hiredis/sds.h"
#include "hybrid/hybrid_scoring.h"
#include "query.h"
#include "query_error.h"
#include "query_flags.h"
#include "query_node.h"
#include "query_types.h"
#include "result_processor.h"
#include "result_processor_ffi.h"
#include "search_options.h"
#include "slot_ranges.h"
#include "slots_tracker_ffi.h"
#include "util/arr/arr.h"

// Record the sub-request's query argument by its index within the held argv.
// Every parse cursor here walks the container's holds, and each sub's holds
// mirror them index-for-index (all hold the same command argv, in order), so
// a cursor-derived offset addresses the same token in the sub's own holds.
// `queryOffset` is in root-cursor coordinates: for a slice cursor, the
// caller adds the slice's base (the root offset recorded before AC_GetSlice).
static void setSubQueryArg(AREQ *sub, uint32_t queryOffset) {
  RS_ASSERT(queryOffset < sub->base.args.argc);
  sub->base.args.queryOffset = queryOffset;
}

// Helper function to set error message with proper plural vs singular form
static void setExpectedArgumentsError(QueryError *status, unsigned int expected, int provided) {
  const char *verb = (provided == 1) ? "was" : "were";
  QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Expected arguments", " %u, but %d %s provided", expected, provided, verb);
}

// Check if we're at the end of arguments in the middle of a clause and set appropriate error for missing argument
static int inline CheckEnd(ArgsCursor *ac, const char *argument, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                    "Missing argument value for ", argument);
      return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

// Remember the first option given that a sparse field does not support, for applyVectorQuery to
// reject once it knows the field's type. `name` is a string literal.
static void noteDenseOnlyOption(ParsedVectorData *pvd, const char *name) {
  if (!pvd->denseOnlyOption) {
    pvd->denseOnlyOption = name;
  }
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

// `lastArm`: no subquery follows SEARCH, so what follows it are the tail's arguments, left for
// HybridParseOptionalArgs rather than rejected here.
static int parseSearchSubquery(ArgsCursor *ac, AREQ *sreq, bool lastArm, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "No query string provided for SEARCH");
    return REDISMODULE_ERR;
  }

  setSubQueryArg(sreq, (uint32_t)ac->offset);
  AC_Advance(ac);
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
    if (lastArm && rv == AC_OK && strcasecmp("DIALECT", cur)) {
      break;
    }
    if (rv == AC_OK && !strcasecmp("DIALECT", cur)) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, DIALECT_ERROR_MSG);
      return REDISMODULE_ERR;
    }

    // Unknown argument that's not VSIM - this is an error
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument", " `%s` in SEARCH", cur);
    return REDISMODULE_ERR;
  }
  if (searchOpts->scoreAlias) {
    AREQ_AddRequestFlags(sreq, QEXEC_F_SEND_SCORES_AS_FIELD);
  }

  return REDISMODULE_OK;
}

static int parseShardKRatioClause(ArgsCursor *ac, ParsedVectorData *pvd,
                                  QueryError *status) {
  // VSIM @vectorfield vector KNN <nargs> ... SHARD_K_RATIO <ratio>
  //                                          ^
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for SHARD_K_RATIO");
    return REDISMODULE_ERR;
  }

  const char *ratioStr;
  size_t ratioStrLen;
  ratioStr = AC_GetStringNC(ac, &ratioStrLen);
  double shardKRatio;
  if (!validateShardKRatio(ratioStr, &shardKRatio, status)) {
    return REDISMODULE_ERR;
  }

  // Add as QueryAttribute (will be applied to VectorQuery via QueryNode_ApplyAttributes)
  QueryAttribute attr = {
    .name = SHARD_K_RATIO_ATTR,
    .namelen = strlen(SHARD_K_RATIO_ATTR),
    .value = rm_strndup(ratioStr, ratioStrLen),
    .vallen = ratioStrLen
  };
  pvd->attributes = array_ensure_append_1(pvd->attributes, attr);
  return REDISMODULE_OK;
}

static int parseRerankClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd,
                             bool *hasRerank, QueryError *status) {
  noteDenseOnlyOption(pvd, "RERANK");
  if (*hasRerank) {
    QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate RERANK argument");
    return REDISMODULE_ERR;
  }
  if (CheckEnd(ac, "RERANK", status) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  size_t valueLen;
  const char *value = AC_GetStringNC(ac, &valueLen);
  addVectorQueryParam(vq, VECSIM_RERANK, sizeof(VECSIM_RERANK) - 1, value, valueLen);
  *hasRerank = true;
  return REDISMODULE_OK;
}

static int parseKNNClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector KNN ...
  //                              ^
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument count");
    return REDISMODULE_ERR;
  }
  // Try to get number of arguments
  unsigned int argumentCount;
  if (AC_GetUnsigned(ac, &argumentCount, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid argument count: expected an unsigned integer");
    return REDISMODULE_ERR;
  } else if (argumentCount == 0 || argumentCount % 2 != 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count", ": %u (must be a positive even number for key/value pairs)", argumentCount);
    return REDISMODULE_ERR;
  }

  bool hasEF = false;
  bool hasRerank = false;
  bool hasShardKRatio = false;
  RS_ASSERT(pvd->vectorScoreFieldAlias == NULL);

  for (int i=0; i<argumentCount; i+=2) {
    if (AC_IsAtEnd(ac)) {
      setExpectedArgumentsError(status, argumentCount, i);
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "K")) {
      if (pvd->hasExplicitK) { // codespell:ignore
        QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate K argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "K", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      long long kValue;
      if (AC_GetLongLong(ac, &kValue, AC_F_GE1) != AC_OK) {
        QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Invalid K value");
        return REDISMODULE_ERR;
      }
      vq->knn.k = (size_t)kValue;
      pvd->hasExplicitK = true;

    } else if (AC_AdvanceIfMatch(ac, "EF_RUNTIME")) {
      noteDenseOnlyOption(pvd, "EF_RUNTIME");
      if (hasEF) {
        QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate EF_RUNTIME argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "EF_RUNTIME", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      long long efValue;
      if (AC_GetLongLong(ac, &efValue, AC_F_GE1 | AC_F_NOADVANCE) != AC_OK) {
        QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Invalid EF_RUNTIME value");
        return REDISMODULE_ERR;
      }
      const char *value;
      size_t valueLen;
      value = AC_GetStringNC(ac, &valueLen);
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EFRUNTIME, strlen(VECSIM_EFRUNTIME), value, valueLen);
      hasEF = true;

    } else if (AC_AdvanceIfMatch(ac, "RERANK")) {
      if (parseRerankClause(ac, vq, pvd, &hasRerank, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }

    } else if (AC_AdvanceIfMatch(ac, "SHARD_K_RATIO")) {
      noteDenseOnlyOption(pvd, "SHARD_K_RATIO");
      if (hasShardKRatio) {
        QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate SHARD_K_RATIO argument");
        return REDISMODULE_ERR;
      }
      if (parseShardKRatioClause(ac, pvd, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
      hasShardKRatio = true;

    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument", " `%s` in KNN", current);
      return REDISMODULE_ERR;
    }
  }
  if (!pvd->hasExplicitK) { // codespell:ignore
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing required argument K");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseRangeClause(ArgsCursor *ac, VectorQuery *vq, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector RANGE ...
  //                                ^
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument count");
    return REDISMODULE_ERR;
  }
  // Try to get number of arguments
  unsigned int argumentCount;
  if (AC_GetUnsigned(ac, &argumentCount, 0) != AC_OK ) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid argument count: expected an unsigned integer");
    return REDISMODULE_ERR;
  } else if (argumentCount == 0 || argumentCount % 2 != 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count", ": %u (must be a positive even number for key/value pairs)", argumentCount);
    return REDISMODULE_ERR;
  }

  bool hasRadius = false;
  bool hasEpsilon = false;
  bool hasRerank = false;
  RS_ASSERT(pvd->vectorScoreFieldAlias == NULL);

  for (int i=0; i<argumentCount; i+=2) {
    if (AC_IsAtEnd(ac)) {
      setExpectedArgumentsError(status, argumentCount, i);
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "RADIUS")) {
      if (hasRadius) {
        QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate RADIUS argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "RADIUS", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      double radiusValue;
      if (AC_GetDouble(ac, &radiusValue, AC_F_GE0) != AC_OK) {
        QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Invalid RADIUS value");
        return REDISMODULE_ERR;
      }
      vq->range.radius = radiusValue;
      hasRadius = true;

    } else if (AC_AdvanceIfMatch(ac, "EPSILON")) {
      if (hasEpsilon) {
        QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate EPSILON argument");
        return REDISMODULE_ERR;
      }
      if (CheckEnd(ac, "EPSILON", status) == REDISMODULE_ERR) return REDISMODULE_ERR;
      double epsilonValue;
      if (AC_GetDouble(ac, &epsilonValue, AC_F_GE0 | AC_F_NOADVANCE) != AC_OK || epsilonValue == 0.0) {
        QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Invalid EPSILON value");
        return REDISMODULE_ERR;
      }
      const char *value;
      size_t valueLen;
      value = AC_GetStringNC(ac, &valueLen);
      // Add directly to VectorQuery params
      addVectorQueryParam(vq, VECSIM_EPSILON, strlen(VECSIM_EPSILON), value, valueLen);
      hasEpsilon = true;

    } else if (AC_AdvanceIfMatch(ac, "RERANK")) {
      if (parseRerankClause(ac, vq, pvd, &hasRerank, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }

    } else {
      const char *current;
      AC_GetString(ac, &current, NULL, AC_F_NOADVANCE);
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument", " `%s` in RANGE", current);
      return REDISMODULE_ERR;
    }
  }
  if (!hasRadius) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing required argument RADIUS");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int parseFilterClause(ArgsCursor *ac, AREQ *vreq, ParsedVectorData *pvd, QueryError *status, unsigned long long count) {
  // VSIM @vectorfield vector [KNN/RANGE ...] [FILTER <count> "filter-expression" [POLICY ADHOC/BATCHES] [BATCH_SIZE batch-size-value]]
  //                                                 ^

  ArgsCursor argCursor= {0};
  // The slice's base in root-cursor coordinates (AC_GetSlice anchors the
  // slice at the parent's current offset, then advances the parent).
  const uint32_t sliceBase = (uint32_t)ac->offset;
  int res = AC_GetSlice(ac, &argCursor, count);
  if (res == AC_ERR_NOARG) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Not enough arguments", " in %s, specified %llu but provided only %u", "FILTER", count, AC_NumRemaining(ac));
    return REDISMODULE_ERR;
  } else if (res != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Bad arguments", " in %s: %s", "FILTER", AC_Strerror(res));
    return REDISMODULE_ERR;
  }

  // Parse filter-expression (required, positional): becomes the sub-request's
  // query argument.
  if (AC_IsAtEnd(&argCursor)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing filter-expression for FILTER");
    return REDISMODULE_ERR;
  }
  setSubQueryArg(vreq, sliceBase + (uint32_t)argCursor.offset);
  AC_Advance(&argCursor);

  // Use ArgParser for optional POLICY and BATCH_SIZE
  ArgParser *parser = ArgParser_New(&argCursor, "FILTER");
  if (!parser) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to create argument parser for FILTER");
    return REDISMODULE_ERR;
  }

  // Parse POLICY (optional, enum)
  const char *policy = NULL;
  static const char *allowedPolicies[] = {"ADHOC", "BATCHES", NULL};
  ArgParser_AddStringV(parser, "POLICY", "Filter policy",
                      &policy, ARG_OPT_OPTIONAL,
                      ARG_OPT_ALLOWED_VALUES, allowedPolicies,
                      ARG_OPT_CALLBACK, handleFilterPolicy, pvd,
                      ARG_OPT_END);

  // Parse BATCH_SIZE (optional)
  const char *batchSize = NULL;
  ArgParser_AddStringV(parser, "BATCH_SIZE", "Batch size",
                      &batchSize, ARG_OPT_OPTIONAL,
                      ARG_OPT_CALLBACK, handleFilterBatchSize, pvd,
                      ARG_OPT_END);

  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return REDISMODULE_ERR;
  }

  ArgParser_Free(parser);
  if (policy) {
    noteDenseOnlyOption(pvd, "POLICY");
  }
  if (batchSize) {
    noteDenseOnlyOption(pvd, "BATCH_SIZE");
  }
  return REDISMODULE_OK;
}

static int parseYieldScoreClause(ArgsCursor *ac, ParsedVectorData *pvd, QueryError *status) {
  // VSIM @vectorfield vector [KNN/RANGE ...] [FILTER ...] YIELD_SCORE_AS <alias>
  //                                                       ^
  if (pvd->vectorScoreFieldAlias != NULL) {
    QueryError_SetError(status, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate YIELD_SCORE_AS argument");
    return REDISMODULE_ERR;
  }
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for YIELD_SCORE_AS");
    return REDISMODULE_ERR;
  }
  const char *value;
  if (AC_GetString(ac, &value, NULL, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "Invalid YIELD_SCORE_AS value");
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

  return REDISMODULE_OK;
}

static int parseVectorSubquery(ArgsCursor *ac, AREQ *vreq, QueryError *status) {
  const uint32_t vsimOffset = (uint32_t)ac->offset;
  // Check for required VSIM keyword
  if (!AC_AdvanceIfMatch(ac, "VSIM")) {
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "VSIM argument is required");
    return REDISMODULE_ERR;
  }

  // Create ParsedVectorData struct at the beginning for cleaner error handling
  ParsedVectorData *pvd = rm_calloc(1, sizeof(ParsedVectorData));
  pvd->argOffset = vsimOffset;
  pvd->queryNodeFlags = QueryNode_YieldsDistance | QueryNode_HybridVectorSubqueryNode;

  // Allocate VectorQuery directly (params arrays will be created lazily by array_ensure_append_1)
  VectorQuery *vq = rm_calloc(1, sizeof(VectorQuery));
  pvd->query = vq;

  // Parse vector field name and store it for later resolution
  const char *fieldNameWithPrefix;
  size_t fieldNameLen;
  if (AC_GetString(ac, &fieldNameWithPrefix, &fieldNameLen, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Missing vector field name");
    goto error;
  }

  // Check if field name starts with @ prefix
  if (fieldNameLen == 0 || fieldNameWithPrefix[0] != '@') {
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Missing @ prefix for vector field name");
    goto error;
  }

  // Skip the @ prefix and store the field name
  pvd->fieldName = fieldNameWithPrefix + 1;

  const char *vectorParam;
  size_t vectorParamLen;
  if (AC_GetString(ac, &vectorParam, &vectorParamLen, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Missing vector argument");
    goto error;
  }

  if (vectorParam[0] == '$') {
    // PARAMETER CASE: store parameter name for later resolution
    vectorParam++;  // Skip '$'
    vectorParamLen--;  // Adjust length for skipped '$'
    pvd->isParameter = true;
  } else {
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Invalid vector argument, expected a parameter name starting with $");
    goto error;
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
    noteDenseOnlyOption(pvd, "RANGE");
    if (parseRangeClause(ac, vq, pvd, status) != REDISMODULE_OK) {
      goto error;
    }
    vq->type = VECSIM_QT_RANGE;
    // Default to BY_SCORE - the iterator returns results sorted by distance.
    // This will be changed to BY_ID below if an explicit FILTER clause is
    // provided, because filtering requires an intersection iterator that uses
    // SkipTo.
    vq->range.order = BY_SCORE;
  }

  // Check for optional FILTER clause - argument may not be in our scope
  if (AC_AdvanceIfMatch(ac, "FILTER")) {
    unsigned long long count = 0;
    if (AC_IsAtEnd(ac)) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument count for FILTER");
      goto error;
    }
    if (AC_GetUnsignedLongLong(ac, &count, 0) != AC_OK) {
      // it's a string, not a number, preserving some degree of backward compatibility
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid filter-expression for FILTER");
        goto error;
      }
      setSubQueryArg(vreq, (uint32_t)ac->offset);
      AC_Advance(ac);
    } else if (parseFilterClause(ac, vreq, pvd, status, count) != REDISMODULE_OK) {
      goto error;
    }

    // RANGE queries with explicit FILTER need BY_ID ordering because the filter
    // creates a PHRASE node which uses an intersection iterator with SkipTo.
    // SkipTo requires child iterators to be sorted by document ID.
    if (vq->type == VECSIM_QT_RANGE) {
      vq->range.order = BY_ID;
    }
  }

  // Check for optional YIELD_SCORE_AS detecting duplicate arguments
  while (!AC_IsAtEnd(ac) && AC_AdvanceIfMatch(ac, "YIELD_SCORE_AS")) {
    if (parseYieldScoreClause(ac, pvd, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  if (AC_AdvanceIfMatch(ac, "DIALECT")) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, DIALECT_ERROR_MSG);
    goto error;
  }

final:
  if (vreq->base.args.queryOffset == QUERY_OFFSET_NONE) {
    pvd->skipFilterIntegration = true;
  }

  // Set vector data in VectorQuery based on type (KNN vs RANGE)
  // The type should be set by now from parseKNNClause or parseRangeClause
  switch (vq->type) {
    case VECSIM_QT_KNN:
      vq->knn.vector = (void*)vectorParam;
      vq->knn.vecLen = vectorParamLen;
      vq->knn.shardWindowRatio = DEFAULT_SHARD_WINDOW_RATIO;
      break;
    case VECSIM_QT_RANGE:
      vq->range.vector = (void*)vectorParam;
      vq->range.vecLen = vectorParamLen;
      break;
  }

  // Set default scoreField using vector field name (can be done during parsing)
  VectorQuery_SetDefaultScoreField(vq, pvd->fieldName, strlen(pvd->fieldName));

  // Snapshot the fields used by EXPLAINSCORE wrapper rendering. applyVectorQuery
  // (called later by AREQ_ApplyContext) transfers `vq` ownership into the AST and
  // nulls pvd->query, so the explain plumbing reads from these copies instead.
  pvd->explainQueryType = vq->type;
  if (vq->type == VECSIM_QT_RANGE) {
    pvd->explainRangeRadius = vq->range.radius;
  }
  for (size_t i = 0; i < array_len(vq->params.params); i++) {
    const VecSimRawParam *p = &vq->params.params[i];
    if (p->name && p->nameLen == strlen(VECSIM_EPSILON) &&
        !strncasecmp(p->name, VECSIM_EPSILON, p->nameLen)) {
      pvd->explainRangeEpsilon = rm_malloc(p->valLen + 1);
      memcpy(pvd->explainRangeEpsilon, p->value, p->valLen);
      pvd->explainRangeEpsilon[p->valLen] = '\0';
      break;
    }
  }

  // Store the completed ParsedVectorData in AREQ
  vreq->parsedVectorData = pvd;

  return REDISMODULE_OK;

error:
  ParsedVectorData_Free(pvd);
  return REDISMODULE_ERR;
}

static void copyCursorConfig(CursorConfig *dest, const CursorConfig *src) {
  dest->maxIdle = src->maxIdle;
  dest->chunkSize = src->chunkSize;
}

// Helper function to copy hybrid request configuration to a single subquery
static void copyHybridConfigToSubquery(AREQ *subqueryRequest,
                                      ParseHybridCommandCtx *parsedCmdCtx,
                                      uint32_t mergeReqflags, size_t maxHybridResults,
                                      ProfileOptions profileOptions) {
  // Copy cursor configuration and flags if cursor is enabled
  if (mergeReqflags & QEXEC_F_IS_CURSOR) {
    // We need to turn on the cursor flag so the cursor id will be sent back when reading from the cursor
    subqueryRequest->reqflags |= QEXEC_F_IS_CURSOR;
    // Copy cursor configuration using the helper function
    copyCursorConfig(&subqueryRequest->base.cursorConfig, parsedCmdCtx->cursorConfig);
  }

  // Copy score sending flags if enabled
  if (mergeReqflags & QEXEC_F_SEND_SCORES) {
    subqueryRequest->reqflags |= QEXEC_F_SEND_SCORES;
  }

  // EXPLAINSCORE only makes sense on the text branch: the search scorer is
  // what allocates the per-result RSScoreExplain that the hybrid merger
  // wraps. The vector branch has no equivalent producer.
  if ((mergeReqflags & QEXEC_F_SEND_SCOREEXPLAIN) &&
      (subqueryRequest->reqflags & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY)) {
    subqueryRequest->reqflags |= QEXEC_F_SEND_SCOREEXPLAIN;
  }

  subqueryRequest->base.reqConfig = *parsedCmdCtx->reqConfig;
  subqueryRequest->base.limits.timeout.config = *parsedCmdCtx->timeoutConfig;

  // Copy max results limits
  subqueryRequest->maxSearchResults = maxHybridResults;
  subqueryRequest->maxAggregateResults = maxHybridResults;

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(subqueryRequest);
  ApplyProfileOptions(qctx, &subqueryRequest->reqflags, profileOptions);
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
    pvd->knnK = pvd->query->knn.k;
  }
}

// Field names for implicit LOAD step
#define HYBRID_IMPLICIT_KEY_FIELDS "@" UNDERSCORE_KEY, "@" UNDERSCORE_SCORE
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
 * Move every load step of the tail plan into each arm's plan, or give each arm an implicit one
 * (the document key and score) when the tail has none.
 */
static void handleLoadStepForHybridPipelines(AGGPlan *tailPlan, AREQ **arms, size_t numArms) {
  PLN_LoadStep *loadStep;
  bool foundAnyLoadStep = false;

  while ((loadStep = (PLN_LoadStep *)AGPLN_FindStep(tailPlan, NULL, NULL, PLN_T_LOAD)) != NULL) {
    foundAnyLoadStep = true;
    AGPLN_PopStep(&loadStep->base);
    for (size_t i = 0; i < numArms; i++) {
      AGPLN_AddStep(&arms[i]->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
    }
    loadStep->base.dtor(&loadStep->base);
  }

  if (!foundAnyLoadStep) {
    loadStep = createImplicitLoadStep();
    for (size_t i = 0; i < numArms; i++) {
      AGPLN_AddStep(&arms[i]->pipeline.ap, &PLNLoadStep_Clone(loadStep)->base);
    }
    loadStep->base.dtor(&loadStep->base);
  }
}

/**
 * Parse the subqueries count at the beginning of the FT.HYBRID command.
 *
 * Expected position in command:
 *   FT.HYBRID <index> <subqueries_count> [SEARCH ...] [VSIM ...]...
 *                    ^
 *
 * The stable syntax has exactly two subqueries, SEARCH then VSIM; the count may be left out for
 * backward compatibility: FT.HYBRID <index> SEARCH <query> VSIM <vector_args>. Any other count,
 * a missing SEARCH and a repeated VSIM are unstable surface.
 *
 * @param ac ArgsCursor for parsing - should be right after the index name
 * @param numArms Output: the number of subqueries the command declares
 * @param status Output parameter for error reporting
 * @return true if parsing succeeded, false if an error occurred (error is set in status)
 */
static bool parseSubqueriesCount(ArgsCursor *ac, size_t *numArms, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing subqueries count for FT.HYBRID");
    return false;
  }

  unsigned int subqueriesCount = 0;
  bool hasSubqueryCount = AC_GetUnsigned(ac, &subqueriesCount, AC_F_GE1) == AC_OK; // Advances the cursor only if parsing succeeded
  const bool unstable = RSGlobalConfig.enableUnstableFeatures;

  const char *next = NULL;
  if (!hasSubqueryCount) {
    // Old format: FT.HYBRID <index> SEARCH <query> VSIM <vector_args>. SEARCH is left for the
    // caller to parse.
    AC_GetString(ac, &next, NULL, AC_F_NOADVANCE);
    if (strcasecmp(next, "SEARCH")) {
      // error according to the new format
      QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX,
                          "Invalid subqueries count: expected an unsigned integer");
      return false;
    }
    *numArms = HYBRID_REQUEST_NUM_SUBQUERIES;
    return true;
  }
  if (!unstable && subqueriesCount != HYBRID_REQUEST_NUM_SUBQUERIES) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "FT.HYBRID currently supports only two subqueries");
    return false;
  }
  if (subqueriesCount > HYBRID_NUM_ARM_SLOTS) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                     "FT.HYBRID supports at most %d subqueries",
                                     HYBRID_NUM_ARM_SLOTS);
    return false;
  }
  next = NULL;
  AC_GetString(ac, &next, NULL, AC_F_NOADVANCE);
  if (!next || (strcasecmp(next, "SEARCH") && (!unstable || strcasecmp(next, "VSIM")))) {
    QueryError_SetError(
        status, QUERY_ERROR_CODE_SYNTAX,
        unstable ? "SEARCH or VSIM keyword is required" : "SEARCH keyword is required");
    return false;
  }
  *numArms = subqueriesCount;
  return true;
}

// The window every arm returns at most, and the merger fuses.
static size_t combineWindow(const HybridPipelineParams *hybridParams) {
  return hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF
             ? hybridParams->scoringCtx->rrfCtx.window
             : hybridParams->scoringCtx->linearCtx.window;
}

// Reorder a per-arm weight array from command order into slot order: `toSlotOrder[i]` is the
// command-order index of the arm now at position i.
static void permuteWeights(double *weights, const size_t *toSlotOrder, size_t numArms) {
  if (!weights) {
    return;
  }
  double inCommandOrder[HYBRID_NUM_ARM_SLOTS];
  memcpy(inCommandOrder, weights, numArms * sizeof(double));
  for (size_t i = 0; i < numArms; i++) {
    weights[i] = inCommandOrder[toSlotOrder[i]];
  }
}

/**
 * Parse FT.HYBRID command arguments and build a complete HybridRequest structure.
 *
 * Expected format: FT.HYBRID <index> [<count>] [SEARCH <query> [SCORER <scorer>]] VSIM
 * <vector_args>... [COMBINE <method> [params]] [aggregation_options]
 *
 * The arms are parsed in command order into the request's pre-allocated sub-requests, then put in
 * slot order (SEARCH, dense VSIM, sparse VSIM) once their fields are resolved; WEIGHTS, given in
 * command order, are reordered with them.
 *
 * Can be called from the main thread or from a background thread. (Note: access RSGlobalConfig
 * which is not thread safe)
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
                       QueryError *status, bool internal, ProfileOptions profileOptions) {
  REDISMODULE_NOT_USED(ctx);
  HybridRequest *hreq = parsedCmdCtx->hreq;
  RS_ASSERT(hreq);
  HybridPipelineParams *hybridParams = parsedCmdCtx->hybridParams;
  hybridParams->scoringCtx = HybridScoringContext_NewDefault();

  // Individual variables used for parsing the tail of the command
  uint32_t *mergeReqflags = &hybridParams->aggregationParams.common.reqflags;

  // Don't expect any flag to be on yet
  RS_ASSERT(*mergeReqflags == 0);
  ApplyProfileFlags(mergeReqflags, profileOptions);

  // Use default dialect if > 1, otherwise use dialect 2
  if (parsedCmdCtx->reqConfig->dialectVersion < MIN_HYBRID_DIALECT) {
    parsedCmdCtx->reqConfig->dialectVersion = MIN_HYBRID_DIALECT;
  }

  RSSearchOptions mergeSearchopts = {0};
  RSSearchOptions_Init(&mergeSearchopts);
  size_t maxHybridResults = RSGlobalConfig.maxSearchResults;

  // Slot ranges info for distributed execution
  const RedisModuleSlotRangeArray *requestSlotRanges = NULL;
  uint32_t keySpaceVersion = INVALID_KEYSPACE_VERSION;

  // Declare variables used after goto statements to avoid "jump skips variable initialization" errors
  AREQ *arms[HYBRID_NUM_ARM_SLOTS] = {0};  // in command order
  PLN_VectorNormalizerStep *vnSteps[HYBRID_NUM_ARM_SLOTS] = {0};
  size_t numArms = 0;
  AREQ *searchRequest = NULL;
  HybridArmMask mask = 0;
  AREQ *slotOrder[HYBRID_NUM_ARM_SLOTS] = {0};
  size_t toSlotOrder[HYBRID_NUM_ARM_SLOTS] = {0};
  AggregationPipelineParams params;
  HybridParseContext hybridParseCtx;

  size_t declaredArms = 0;
  if (!parseSubqueriesCount(ac, &declaredArms, status)) {
    goto error;
  }

  // parseSubqueriesCount bounds declaredArms to [1, HYBRID_NUM_ARM_SLOTS]; the second condition
  // states that bound where `arms` is written.
  while (numArms < declaredArms && numArms < HYBRID_NUM_ARM_SLOTS) {
    if (numArms == array_len(hreq->requests)) {
      // Construction allocated one sub-request per SEARCH or VSIM keyword, so the next token is
      // not an arm, unless ENABLE_UNSTABLE_FEATURES was turned on between allocation and parsing.
      const bool isArm = AC_AdvanceIfMatch(ac, "VSIM") || AC_AdvanceIfMatch(ac, "SEARCH");
      QueryError_SetError(
          status, isArm ? QUERY_ERROR_CODE_PARSE_ARGS : QUERY_ERROR_CODE_SYNTAX,
          isArm ? "Too many subqueries for FT.HYBRID" : "VSIM argument is required");
      goto error;
    }
    AREQ *arm = hreq->requests[numArms];
    arm->base.reqConfig.dialectVersion = parsedCmdCtx->reqConfig->dialectVersion;
    if (internal) {
      arm->reqflags |= QEXEC_F_INTERNAL;
    }
    // SEARCH may come anywhere among the arms, once.
    if (!searchRequest && AC_AdvanceIfMatch(ac, "SEARCH")) {
      searchRequest = arm;
      arm->reqflags |= QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY;
      arm->ast.validationFlags |= QAST_NO_VECTOR;
      if (parseSearchSubquery(ac, arm, numArms + 1 == declaredArms, status) != REDISMODULE_OK) {
        goto error;
      }
    } else {
      arm->reqflags |= QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY;
      arm->ast.validationFlags |= QAST_NO_WEIGHT | QAST_NO_VECTOR;
      if (parseVectorSubquery(ac, arm, status) != REDISMODULE_OK) {
        goto error;
      }
    }
    arms[numArms++] = arm;
  }
  // Without unstable features an extra arm is left for HybridParseOptionalArgs to reject, as it
  // always was.
  RS_LOG_ASSERT(numArms > 0 && numArms == declaredArms, "every declared arm was parsed");
  if (RSGlobalConfig.enableUnstableFeatures &&
      (AC_AdvanceIfMatch(ac, "VSIM") || AC_AdvanceIfMatch(ac, "SEARCH"))) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                     "FT.HYBRID declares %zu subqueries but has more",
                                     declaredArms);
    goto error;
  }

  hybridParseCtx = (HybridParseContext){
      .status = status,
      .specifiedArgs = 0,
      .hybridScoringCtx = hybridParams->scoringCtx,
      .numSubqueries = numArms,
      .plan = parsedCmdCtx->tailPlan,
      .reqFlags = mergeReqflags,
      .searchopts = &mergeSearchopts,
      .cursorConfig = parsedCmdCtx->cursorConfig,
      .reqConfig = parsedCmdCtx->reqConfig,
      .timeoutConfig = parsedCmdCtx->timeoutConfig,
      .maxResults = &maxHybridResults,
      .querySlots = &requestSlotRanges,
      .keySpaceVersion = &keySpaceVersion,
      .coordDispatchTime = parsedCmdCtx->coordDispatchTime,
  };
  if (HybridParseOptionalArgs(&hybridParseCtx, ac, internal) != REDISMODULE_OK) {
    goto error;
  }

  // Record whether TIMEOUT was explicitly given, and capture the client's value
  // BEFORE the foreground-timeout cap below, so the coordinator can forward the
  // exact client timeout to shards without re-scanning argv.
  parsedCmdCtx->timeoutSpecified = (hybridParseCtx.specifiedArgs & SPECIFIED_ARG_TIMEOUT) != 0;
  parsedCmdCtx->clientTimeoutMS = parsedCmdCtx->timeoutConfig->queryTimeoutMS;

  // Cap the effective query timeout to search-_max-foreground-timeout-limit
  // when the limit is active. The hybrid request has a timeout configuration
  // that is later copied into every subquery through copyHybridConfigToSubquery.
  // finishSendChunkReply_hybrid reads the cap flag from the first subquery, which
  // may be any arm, so the flag is set on all of them.
  if (RSConfig_CapQueryTimeoutToForegroundLimit(&parsedCmdCtx->timeoutConfig->queryTimeoutMS)) {
    for (size_t i = 0; i < numArms; i++) {
      arms[i]->stateflags |= QEXEC_S_MAX_TIMEOUT_CAPPED;
    }
  }

  // Set slots info in every subquery
  if (internal) {
    if (!requestSlotRanges) {
      // Coordinators older than 8.4 do not send a slots specification. Fall back to the
      // current local slots so rolling upgrades across the 8.4 boundary keep working.
      requestSlotRanges = ASM_FallbackToLocalSlots(ctx, &keySpaceVersion);
    }
    for (size_t i = 0; i < numArms; i++) {
      // The last arm takes the parsed ranges, the others a clone each.
      arms[i]->querySlots =
          i + 1 < numArms ? SlotRangeArray_Clone(requestSlotRanges) : requestSlotRanges;
      arms[i]->keySpaceVersion = keySpaceVersion;
      if (keySpaceVersion != INVALID_KEYSPACE_VERSION) {
        ASM_KeySpaceVersionTracker_IncreaseQueryCount(keySpaceVersion);
      }
    }
    requestSlotRanges = NULL; // ownership transferred
  }

  for (size_t i = 0; i < numArms; i++) {
    AREQ *arm = arms[i];
    if (arm == searchRequest) {
      continue;
    }
    ParsedVectorData *armPvd = arm->parsedVectorData;
    // If YIELD_SCORE_AS was specified, use its string (pass ownership from pvd to vnStep),
    // otherwise, store the vector score in a default key.
    const char *vectorScoreFieldAlias;
    if (armPvd->vectorScoreFieldAlias != NULL) {
      vectorScoreFieldAlias = armPvd->vectorScoreFieldAlias;
      armPvd->vectorScoreFieldAlias = NULL;
    } else {
      vectorScoreFieldAlias =
          VectorQuery_GetDefaultScoreFieldName(armPvd->fieldName, strlen(armPvd->fieldName));
      armPvd->queryNodeFlags |= QueryNode_HideVectorDistanceField;
    }
    // Store the key string so it could fetch the distance from the RlookupRow
    vnSteps[i] = PLNVectorNormalizerStep_New(armPvd->fieldName, vectorScoreFieldAlias);
    AGPLN_AddStep(&arm->pipeline.ap, &vnSteps[i]->base);
  }

  // Copy hybrid request configuration to each subquery
  for (size_t i = 0; i < numArms; i++) {
    copyHybridConfigToSubquery(arms[i], parsedCmdCtx, *mergeReqflags, maxHybridResults,
                               profileOptions);
  }

  // Each subquery frees its own params dict: the last takes the original, the others a clone.
  if (mergeSearchopts.params) {
    for (size_t i = 0; i + 1 < numArms; i++) {
      arms[i]->searchopts.params = Param_DictClone(mergeSearchopts.params);
    }
    arms[numArms - 1]->searchopts.params = mergeSearchopts.params;
    mergeSearchopts.params = NULL;
  }

  if ((hybridParseCtx.specifiedArgs & ~SPECIFIED_ARG_COMBINE) != 0) {
    *mergeReqflags |= QEXEC_F_IS_HYBRID_TAIL;

    for (size_t i = 0; i < numArms; i++) {
      if (arms[i] != searchRequest &&
          QAST_EvalParams(&arms[i]->ast, &arms[i]->searchopts, 2, status) != REDISMODULE_OK) {
        goto error;
      }
    }
  }

  // In the search subquery we want the sorter result processor to be in the upstream of the loader
  // This is because the sorter limits the number of results and can reduce the amount of work the loader needs to do
  // So it is important this is done before we add the load step to the subqueries plan
  if (searchRequest) {
    AGPLN_GetOrCreateArrangeStep(&searchRequest->pipeline.ap)->limit = combineWindow(hybridParams);
  }

  // Handle load step distribution for hybrid pipelines
  handleLoadStepForHybridPipelines(parsedCmdCtx->tailPlan, arms, numArms);

  if (!(*mergeReqflags & QEXEC_F_NO_SORT)) {
    // No NOSORT - add implicit sort-by-score
    AGPLN_GetOrCreateArrangeStep(parsedCmdCtx->tailPlan);
  }

  // Apply KNN K ≤ WINDOW constraint after all argument resolution is complete
  for (size_t i = 0; i < numArms; i++) {
    applyKNNTopKWindowConstraint(arms[i]->parsedVectorData, hybridParams);
  }

  if (hybridParseCtx.nprefixes &&
      !IndexSpec_IsCoherent(arms[0]->sctx->spec, hybridParseCtx.prefixes,
                            hybridParseCtx.nprefixes)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_MISMATCH, NULL);
    goto error;
  }

  // Apply context to each request. A VSIM arm's slot is known only once applyVectorQuery has
  // resolved its field.
  for (size_t i = 0; i < numArms; i++) {
    AREQ *arm = arms[i];
    if (AREQ_ApplyContext(arm, arm->sctx, status) != REDISMODULE_OK) {
      AddValidationErrorContext(arm, status);
      goto error;
    }
    HybridArmSlot slot = SEARCH_INDEX;
    if (arm != searchRequest) {
      slot = arm->parsedVectorData->armSlot;
      RS_ASSERT(slot == VECTOR_INDEX || slot == SPARSE_INDEX);
    }
    if (HybridArmMask_Has(mask, slot)) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                          "FT.HYBRID accepts at most one VSIM subquery on a VECTOR field and one "
                          "on a SPARSEVECTOR field");
      goto error;
    }
    mask |= HybridArm_Bit(slot);
    toSlotOrder[slot] = i;
  }
  // Compact toSlotOrder over the slots in use, so entry k names the arm at position k.
  for (size_t pos = 0; pos < numArms; pos++) {
    const HybridArmSlot slot = HybridArmMask_SlotAt(mask, pos);
    toSlotOrder[pos] = toSlotOrder[slot];
    slotOrder[pos] = arms[toSlotOrder[pos]];
  }

  for (size_t i = 0; i < numArms; i++) {
    const ParsedVectorData *pvd = arms[i]->parsedVectorData;
    if (arms[i] == searchRequest || pvd->armSlot != SPARSE_INDEX) {
      continue;
    }
    // VecSim stops a dense KNN after K results, but a sparse node yields every document that
    // shares a dimension with the query. Keeping the K best normalized scores gives the arm the
    // top K that FT.SEARCH returns. It goes right after the normalizer, which sets the score, so
    // only those K are loaded. It stays out of the shard command: each shard parses the VSIM clause
    // and adds its own, and the coordinator's copy is set up by setupCoordinatorArrangeSteps.
    AGGPlan *plan = &arms[i]->pipeline.ap;
    PLN_ArrangeStep *topK = AGPLN_GetOrCreateArrangeStep(plan);
    AGPLN_PopStep(&topK->base);
    AGPLN_AddAfter(plan, &vnSteps[i]->base, &topK->base);
    topK->limit = pvd->knnK;
    topK->runLocal = true;
  }

  // WEIGHTS were given in command order; the merger reads them in slot order.
  if (hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
    permuteWeights(hybridParams->scoringCtx->rrfCtx.weights, toSlotOrder, numArms);
  } else {
    permuteWeights(hybridParams->scoringCtx->linearCtx.linearWeights, toSlotOrder, numArms);
  }

  // Build here, after AREQ_ApplyContext on every sub-query: it has resolved the
  // search scorer name, and the vector ParsedVectorData snapshots are still intact
  // (the live VectorQuery is moved into the AST by the same call, so reading it
  // any later would be too late).
  if (*mergeReqflags & QEXEC_F_SEND_SCOREEXPLAIN) {
    hybridParams->explainCtx = HybridExplainContext_Build((const AREQ *const *)slotOrder, numArms);
  }

  // thread safe context
  params = (AggregationPipelineParams){
      .common =
          {
              .sctx = sctx,  // should be a separate context?
              .reqflags = *mergeReqflags | QEXEC_F_IS_HYBRID_TAIL,
              .optimizer = NULL,  // is it?
              .scoreAlias = mergeSearchopts.scoreAlias,
          },
      .outFields = NULL,
      .maxResultsLimit = maxHybridResults,
      .groupByLimits = GroupByLimits_Default(RSGlobalConfig.maxAggregateGroups),
      // AREQ_ApplyContext resolved every arm's language to the index default unless SEARCH set one.
      .language = (searchRequest ? searchRequest : arms[0])->searchopts.language,
      .diskScoreSkippingQuery = parsedCmdCtx->reqConfig->diskScoreSkippingQuery,
  };

  hybridParams->aggregationParams = params;

  HybridRequest_SetArms(hreq, slotOrder, numArms, mask);
  parsedCmdCtx->armMask = mask;
  parsedCmdCtx->search = searchRequest;
  parsedCmdCtx->vector = NULL;
  for (size_t i = 0; i < numArms; i++) {
    if (slotOrder[i] != searchRequest) {
      parsedCmdCtx->vector = slotOrder[i];
      break;
    }
  }
  return REDISMODULE_OK;

error:
  if (requestSlotRanges) {
    rm_free((void *)requestSlotRanges);
  }
  if (mergeSearchopts.params) {
    Param_DictFree(mergeSearchopts.params);
  }
  HybridPipelineParams_Cleanup(hybridParams);
  return REDISMODULE_ERR;
}
