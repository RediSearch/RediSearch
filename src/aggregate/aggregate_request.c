/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "aggregate.h"
#include "reducer.h"

#include <query.h>
#include <extension.h>
#include <result_processor.h>
#include <util/arr.h>
#include <rmutil/util.h>
#include "ext/default.h"
#include "extension.h"
#include "profile.h"
#include "config.h"
#include "util/timeout.h"
#include "query_optimizer.h"
#include "resp3.h"
#include "obfuscation/hidden.h"
#include "hybrid/vector_query_utils.h"
#include "vector_index.h"

extern RSConfig RSGlobalConfig;

/**
 * Ensures that the user has not requested one of the 'extended' features. Extended
 * in this case refers to reducers which re-create the search results.
 * @param req the request
 * @return true if the request is in simple mode, false otherwise
 */
static bool ensureSimpleMode(AREQ *req) {
  if (AREQ_RequestFlags(req) & QEXEC_F_IS_AGGREGATE) {
    return false;
  }
  AREQ_AddRequestFlags(req, QEXEC_F_IS_SEARCH);
  return true;
}

/**
 * Like @ref ensureSimpleMode(), but does the opposite -- ensures that one of the
 * 'simple' options - i.e. ones which rely on the field to be the exact same as
 * found in the document - was not requested.
 * name argument must not contain any user data, as it is used for error formatting
*/
static int ensureExtendedMode(uint32_t *reqflags, const char *name, QueryError *status) {
  if (*reqflags & QEXEC_F_IS_SEARCH) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
                           "option `%s` is mutually exclusive with simple (i.e. search) options",
                           name);
    return 0;
  }
  REQFLAGS_AddFlags(reqflags, QEXEC_F_IS_AGGREGATE);
  return 1;
}

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, ParseAggPlanContext *papCtx);

/**
 * Initialize basic AREQ structure with search options and aggregation plan.
 */
void initializeAREQ(AREQ *req) {
  AGPLN_Init(AREQ_AGGPlan(req));
  RSSearchOptions_Init(&req->searchopts);
}

static void ReturnedField_Free(ReturnedField *field) {
  rm_free(field->highlightSettings.openTag);
  rm_free(field->highlightSettings.closeTag);
  rm_free(field->summarizeSettings.separator);
}

void FieldList_Free(FieldList *fields) {
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    ReturnedField_Free(fields->fields + ii);
  }
  ReturnedField_Free(&fields->defaultField);
  rm_free(fields->fields);
}

ReturnedField *FieldList_GetCreateField(FieldList *fields, const char *name, const char *path) {
  size_t foundIndex = -1;
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    if (!strcmp(fields->fields[ii].name, name)) {
      return fields->fields + ii;
    }
  }

  fields->fields = rm_realloc(fields->fields, sizeof(*fields->fields) * ++fields->numFields);
  ReturnedField *ret = fields->fields + (fields->numFields - 1);
  memset(ret, 0, sizeof *ret);
  ret->name = name;
  ret->path = path ? path : name;
  return ret;
}

static void FieldList_RestrictReturn(FieldList *fields) {
  size_t oix = 0;
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    if (fields->fields[ii].explicitReturn == 0) {
      ReturnedField_Free(fields->fields + ii);
    } else if (ii != oix) {
      fields->fields[oix++] = fields->fields[ii];
    } else {
      ++oix;
    }
  }
  fields->numFields = oix;
}

static int parseCursorSettings(uint32_t *reqflags, CursorConfig *cursorConfig, ArgsCursor *ac, QueryError *status) {
  ACArgSpec specs[] = {{.name = "MAXIDLE",
                        .type = AC_ARGTYPE_UINT,
                        .target = &cursorConfig->maxIdle,
                        .intflags = AC_F_GE1},
                       {.name = "COUNT",
                        .type = AC_ARGTYPE_UINT,
                        .target = &cursorConfig->chunkSize,
                        .intflags = AC_F_GE1},
                       {NULL}};

  int rv;
  ACArgSpec *errArg = NULL;
  if ((rv = AC_ParseArgSpec(ac, specs, &errArg)) != AC_OK && rv != AC_ERR_ENOENT) {
    QERR_MKBADARGS_AC(status, errArg->name, rv);
    return REDISMODULE_ERR;
  }

  if (cursorConfig->maxIdle == 0 || cursorConfig->maxIdle > RSGlobalConfig.cursorMaxIdle) {
    cursorConfig->maxIdle = RSGlobalConfig.cursorMaxIdle;
  }
  REQFLAGS_AddFlags(reqflags, QEXEC_F_IS_CURSOR);
  return REDISMODULE_OK;
}

static int parseRequiredFields(const char ***requiredFields, ArgsCursor *ac, QueryError *status){

  ArgsCursor args = {0};
  int rv = AC_GetVarArgs(ac, &args);
  if (rv != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for _REQUIRED_FIELDS: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }
  int requiredFieldNum = AC_NumArgs(&args);
  // This array contains shallow copy of the required fields names. Those copies are to use only for lookup.
  // If we need to use them in reply we should make a copy of those strings.
  const char** reqFields = array_new(const char*, requiredFieldNum);
  for(size_t i=0; i < requiredFieldNum; i++) {
    const char *s = AC_GetStringNC(&args, NULL); {
      if(!s) {
        array_free(reqFields);
        return REDISMODULE_ERR;
      }
    }
    array_append(reqFields, s);
  }

  *requiredFields = reqFields;

  return REDISMODULE_OK;
}

int parseDialect(unsigned int *dialect, ArgsCursor *ac, QueryError *status) {
  if (AC_NumRemaining(ac) < 1) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Need an argument for DIALECT");
      return REDISMODULE_ERR;
    }
    if ((AC_GetUnsigned(ac, dialect, AC_F_GE1) != AC_OK) || (*dialect > MAX_DIALECT_VERSION)) {
      QueryError_SetWithoutUserDataFmt(
        status, QUERY_ERROR_CODE_PARSE_ARGS,
        "DIALECT requires a non negative integer >=%u and <= %u",
        MIN_DIALECT_VERSION, MAX_DIALECT_VERSION
      );
      return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

// Parse the available formats for search result values: FORMAT STRING|EXPAND
int parseValueFormat(uint32_t *flags, ArgsCursor *ac, QueryError *status) {
  const char *format;
  int rv = AC_GetString(ac, &format, NULL, 0);
  if (rv != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "Need an argument for FORMAT");
    return REDISMODULE_ERR;
  }
  if (!strcasecmp(format, "EXPAND")) {
    *flags |= QEXEC_FORMAT_EXPAND;
  } else if (!strcasecmp(format, "STRING")) {
    *flags &= ~QEXEC_FORMAT_EXPAND;
  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "FORMAT", " %s is not supported", format);
    return REDISMODULE_ERR;
  }
  *flags &= ~QEXEC_FORMAT_DEFAULT;
  return REDISMODULE_OK;
}

// Parse the timeout value
int parseTimeout(long long *timeout, ArgsCursor *ac, QueryError *status) {
  if (AC_NumRemaining(ac) < 1) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Need an argument for TIMEOUT");
    return REDISMODULE_ERR;
  }

  if (AC_GetLongLong(ac, timeout, AC_F_GE0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
      "TIMEOUT requires a non negative integer.");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

int SetValueFormat(bool is_resp3, bool is_json, uint32_t *flags, QueryError *status) {
  if (*flags & QEXEC_FORMAT_DEFAULT) {
    *flags &= ~QEXEC_FORMAT_EXPAND;
    *flags &= ~QEXEC_FORMAT_DEFAULT;
  }

  if (*flags & QEXEC_FORMAT_EXPAND) {
    if (!is_resp3) {
      QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "EXPAND format is only supported with RESP3");
      return REDISMODULE_ERR;
    }
    if (!is_json) {
      QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "EXPAND format is only supported with JSON");
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

void SetSearchCtx(RedisSearchCtx *sctx, const AREQ *req) {
  if (AREQ_RequestFlags(req) & QEXEC_FORMAT_EXPAND) {
    sctx->expanded = 1;
    sctx->apiVersion = MAX(APIVERSION_RETURN_MULTI_CMP_FIRST, req->reqConfig.dialectVersion);
  } else {
    sctx->apiVersion = req->reqConfig.dialectVersion;
  }
}

#define ARG_HANDLED 1
#define ARG_ERROR -1
#define ARG_UNKNOWN 0

static int handleCommonArgs(ParseAggPlanContext *papCtx, ArgsCursor *ac, QueryError *status) {
  int rv;
  bool dialect_specified = false;
  // This handles the common arguments that are not stateful
  if (AC_AdvanceIfMatch(ac, "LIMIT")) {
    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(papCtx->plan);
    arng->isLimited = 1;
    // Parse offset, length
    if (AC_NumRemaining(ac) < 2) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "LIMIT requires two arguments");
      return ARG_ERROR;
    }
    if ((rv = AC_GetU64(ac, &arng->offset, 0)) != AC_OK ||
        (rv = AC_GetU64(ac, &arng->limit, 0)) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "LIMIT needs two numeric arguments");
      return ARG_ERROR;
    }

    if (arng->limit == 0 && arng->offset != 0) {
      QueryError_SetError(status, QUERY_ERROR_CODE_LIMIT, "The `offset` of the LIMIT must be 0 when `num` is 0");
      return ARG_ERROR;
    }

    if (arng->isLimited && arng->limit == 0) {
      // LIMIT 0 0 - only count
      REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_NOROWS);
      REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_SEND_NOFIELDS);
      // TODO: unify if when req holds only maxResults according to the query type.
      //(SEARCH / AGGREGATE)
    } else if ((arng->limit > *papCtx->maxSearchResults) && (*papCtx->reqflags & (QEXEC_F_IS_SEARCH))) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "LIMIT exceeds maximum of %llu",
                             *papCtx->maxSearchResults);
      return ARG_ERROR;
    } else if ((arng->limit > *papCtx->maxAggregateResults) && !(*papCtx->reqflags & (QEXEC_F_IS_SEARCH))) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "LIMIT exceeds maximum of %llu",
                             *papCtx->maxAggregateResults);
      return ARG_ERROR;
    } else if (arng->offset > *papCtx->maxSearchResults) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "OFFSET exceeds maximum of %llu",
                             *papCtx->maxSearchResults);
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "SORTBY")) {
    const char *firstArg;
    bool isSortby0 = AC_GetString(ac, &firstArg, NULL, AC_F_NOADVANCE) == AC_OK
                        && !strcmp(firstArg, "0");
    if (isSortby0 && ((*papCtx->reqflags & QEXEC_F_IS_HYBRID_TAIL) || (*papCtx->reqflags & QEXEC_F_IS_AGGREGATE))) {
      AC_Advance(ac);  // Advance without adding SortBy step to the plan
      *papCtx->reqflags |= QEXEC_F_NO_SORT;
    } else {
      REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_SORTBY);
      PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(papCtx->plan);
      if (parseSortby(arng, ac, status, papCtx) != REDISMODULE_OK) {
        return ARG_ERROR;
      }
    }
  } else if (AC_AdvanceIfMatch(ac, "TIMEOUT")) {
    if (AC_NumRemaining(ac) < 1) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Need argument for TIMEOUT");
      return ARG_ERROR;
    }
    if (AC_GetLongLong(ac, &papCtx->reqConfig->queryTimeoutMS, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "TIMEOUT requires a non negative integer");
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "WITHCURSOR")) {
    if (parseCursorSettings(papCtx->reqflags, papCtx->cursorConfig, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "_NUM_SSTRING")) {
    REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_TYPED);
  } else if (AC_AdvanceIfMatch(ac, "WITHRAWIDS")) {
    REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_SENDRAWIDS);
  } else if (AC_AdvanceIfMatch(ac, "PARAMS")) {
    if (parseParams(&(papCtx->searchopts->params), ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if(AC_AdvanceIfMatch(ac, "_REQUIRED_FIELDS") && papCtx->requiredFields) {
    if (parseRequiredFields(papCtx->requiredFields, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
    REQFLAGS_AddFlags(papCtx->reqflags, QEXEC_F_REQUIRED_FIELDS);
  } else if(AC_AdvanceIfMatch(ac, "DIALECT")) {
    dialect_specified = true;
    if (parseDialect(&papCtx->reqConfig->dialectVersion, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if(AC_AdvanceIfMatch(ac, "FORMAT")) {
    if (parseValueFormat(papCtx->reqflags, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "_INDEX_PREFIXES") && papCtx->prefixesOffset) {
    // Set the offset of the prefixes in the query, for further processing later
    *papCtx->prefixesOffset = ac->offset - 1;

    ArgsCursor tmp = {0};
    if (AC_GetVarArgs(ac, &tmp) != AC_OK) {
      RS_LOG_ASSERT(false, "Bad arguments for _INDEX_PREFIXES (coordinator)");
    }
  } else if (AC_AdvanceIfMatch(ac, "BM25STD_TANH_FACTOR")) {
    if (AC_NumRemaining(ac) < 1) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Need an argument for BM25STD_TANH_FACTOR");
      return ARG_ERROR;
    }
    if (AC_GetUnsignedLongLong(ac, (unsigned long long *)&papCtx->reqConfig->BM25STD_TanhFactor, AC_F_GE1) != AC_OK) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "BM25STD_TANH_FACTOR must be between %d and %d inclusive",
      BM25STD_TANH_FACTOR_MIN, BM25STD_TANH_FACTOR_MAX);
      return ARG_ERROR;
    }
  } else if ((*papCtx->reqflags & QEXEC_F_INTERNAL) && AC_AdvanceIfMatch(ac, SLOTS_STR)) {
    if (*papCtx->querySlots) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SLOTS_STR" already specified");
      return ARG_ERROR;
    }
    if (AC_NumRemaining(ac) < 1) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SLOTS_STR" missing argument");
      return ARG_ERROR;
    }
    size_t serialization_len;
    const char *serialization = AC_GetStringNC(ac, &serialization_len);
    RedisModuleSlotRangeArray *slot_array = SlotRangesArray_Deserialize(serialization, serialization_len);
    if (!slot_array) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to deserialize "SLOTS_STR" data");
      return ARG_ERROR;
    }
    // TODO ASM: check if the requested slots are available
    *papCtx->querySlots = slot_array;
    *papCtx->slotsVersion = 0;
  } else {
    return ARG_UNKNOWN;
  }

  if (dialect_specified && papCtx->reqConfig->dialectVersion < APIVERSION_RETURN_MULTI_CMP_FIRST && *papCtx->reqflags & QEXEC_FORMAT_EXPAND) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "EXPAND format requires dialect %u or greater", APIVERSION_RETURN_MULTI_CMP_FIRST);
    return ARG_ERROR;
  }

  return ARG_HANDLED;
}

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, ParseAggPlanContext *papCtx) {
  bool isLegacy = *papCtx->reqflags & QEXEC_F_IS_SEARCH;
  // Prevent multiple SORTBY steps
  if (arng->sortKeys != NULL) {
    if (isLegacy) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Multiple SORTBY steps are not allowed");
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Multiple SORTBY steps are not allowed. Sort multiple fields in a single step");
    }
    return REDISMODULE_ERR;
  }

  // Assume argument is at 'SORTBY'
  ArgsCursor subArgs = {0};
  int rv;
  int legacyDesc = 0;

  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 0 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = SORTASCMAP_INIT;
  const char **keys = NULL;

  if (isLegacy) {
    if (AC_NumRemaining(ac) > 0) {
      // Mimic subArgs to contain the single field we already have
      AC_GetSlice(ac, &subArgs, 1);
      if (AC_AdvanceIfMatch(ac, "DESC")) {
        legacyDesc = 1;
      } else if (AC_AdvanceIfMatch(ac, "ASC")) {
        legacyDesc = 0;
      }
    } else {
      goto err;
    }
  } else {
    rv = AC_GetVarArgs(ac, &subArgs);
    if (rv != AC_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for SORTBY: %s", AC_Strerror(rv));
      goto err;
    }
  }

  keys = array_new(const char *, 8);

  if (isLegacy) {
    // Legacy demands one field and an optional ASC/DESC parameter. Both
    // of these are handled above, so no need for argument parsing
    const char *s = AC_GetStringNC(&subArgs, NULL);
    array_append(keys, s);

    if (legacyDesc) {
      SORTASCMAP_SETDESC(ascMap, 0);
    }
  } else {
    while (!AC_IsAtEnd(&subArgs)) {

      const char *s = AC_GetStringNC(&subArgs, NULL);
      if (*s == '@') {
        if (array_len(keys) >= SORTASCMAP_MAXFIELDS) {
          QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Cannot sort by more than %lu fields", SORTASCMAP_MAXFIELDS);
          goto err;
        }
        s++;
        array_append(keys, s);
        continue;
      }

      if (!strcasecmp(s, "ASC")) {
        SORTASCMAP_SETASC(ascMap, array_len(keys) - 1);
      } else if (!strcasecmp(s, "DESC")) {
        SORTASCMAP_SETDESC(ascMap, array_len(keys) - 1);
      } else {
        // Unknown token - neither a property nor ASC/DESC
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "MISSING ASC or DESC after sort field", " (%s)", s);
        goto err;
      }
    }
  }

  // Parse optional MAX
  // MAX is not included in the normal SORTBY arglist.. so we need to switch
  // back to `ac`
  if (AC_AdvanceIfMatch(ac, "MAX")) {
    if (isLegacy) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "SORTBY MAX is not supported by FT.SEARCH");
      goto err;
    }
    unsigned mx = 0;
    if ((rv = AC_GetUnsigned(ac, &mx, 0) != AC_OK)) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for MAX: %s", AC_Strerror(rv));
      goto err;
    }
    arng->limit = mx;
  }

  arng->sortAscMap = ascMap;
  arng->sortKeys = keys;
  return REDISMODULE_OK;
err:
  QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad SORTBY arguments");
  if (keys) {
    array_free(keys);
  }
  return REDISMODULE_ERR;
}

static int parseQueryLegacyArgs(ArgsCursor *ac, RSSearchOptions *options, bool *hasEmptyFilterValue, QueryError *status) {
  if (AC_AdvanceIfMatch(ac, "FILTER")) {
    // Numeric filter
    LegacyNumericFilter **curpp = array_ensure_tail(&options->legacy.filters, LegacyNumericFilter *);
    *curpp = NumericFilter_LegacyParse(ac, hasEmptyFilterValue, status);
    if (!*curpp) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "GEOFILTER")) {
    LegacyGeoFilter *cur_gf = rm_new(*cur_gf);
    array_ensure_append_1(options->legacy.geo_filters, cur_gf);
    if (GeoFilter_LegacyParse(cur_gf, ac, hasEmptyFilterValue, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else {
    return ARG_UNKNOWN;
  }
  return ARG_HANDLED;
}

static int parseQueryArgs(ArgsCursor *ac, AREQ *req, RSSearchOptions *searchOpts,
                          QueryAST *ast, AggregatePlan *plan, QueryError *status) {
  // Parse query-specific arguments..
  const char *languageStr = NULL;
  ArgsCursor returnFields = {0};
  ArgsCursor inKeys = {0};
  ArgsCursor inFields = {0};
  Pipeline *pipeline = &req->pipeline;
  ACArgSpec querySpecs[] = {
      {.name = "INFIELDS", .type = AC_ARGTYPE_SUBARGS, .target = &inFields},  // Comment
      {.name = "SLOP",
       .type = AC_ARGTYPE_INT,
       .target = &searchOpts->slop,
       .intflags = AC_F_COALESCE},
      {.name = "LANGUAGE", .type = AC_ARGTYPE_STRING, .target = &languageStr},
      {.name = "EXPANDER", .type = AC_ARGTYPE_STRING, .target = &searchOpts->expanderName},
      {.name = "INKEYS", .type = AC_ARGTYPE_SUBARGS, .target = &inKeys},
      {.name = "SCORER", .type = AC_ARGTYPE_STRING, .target = &searchOpts->scorerName},
      {.name = "RETURN", .type = AC_ARGTYPE_SUBARGS, .target = &returnFields},
      {AC_MKBITFLAG("INORDER", &searchOpts->flags, Search_InOrder)},
      {AC_MKBITFLAG("VERBATIM", &searchOpts->flags, Search_Verbatim)},
      {AC_MKBITFLAG("WITHSCORES", &req->reqflags, QEXEC_F_SEND_SCORES)},
      {AC_MKBITFLAG("ADDSCORES", &req->reqflags, QEXEC_F_SEND_SCORES_AS_FIELD)},
      {AC_MKBITFLAG("WITHSORTKEYS", &req->reqflags, QEXEC_F_SEND_SORTKEYS)},
      {AC_MKBITFLAG("WITHPAYLOADS", &req->reqflags, QEXEC_F_SEND_PAYLOADS)},
      {AC_MKBITFLAG("NOCONTENT", &req->reqflags, QEXEC_F_SEND_NOFIELDS)},
      {AC_MKBITFLAG("NOSTOPWORDS", &searchOpts->flags, Search_NoStopWords)},
      {AC_MKBITFLAG("EXPLAINSCORE", &req->reqflags, QEXEC_F_SEND_SCOREEXPLAIN)},
      {.name = "PAYLOAD",
       .type = AC_ARGTYPE_STRING,
       .target = &ast->udata,
       .len = &ast->udatalen},
      {NULL}};

  AREQ_AddRequestFlags(req, QEXEC_FORMAT_DEFAULT);
  bool optimization_specified = false;
  bool hasEmptyFilterValue = false;
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

    // See if this is one of our arguments which requires special handling
    if (AC_AdvanceIfMatch(ac, "SUMMARIZE")) {
      if(!ensureSimpleMode(req)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "SUMMARIZE is not supported on FT.AGGREGATE");
        return REDISMODULE_ERR;
      }
      if (ParseSummarize(ac, &req->outFields) == REDISMODULE_ERR) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for SUMMARIZE");
        return REDISMODULE_ERR;
      }
      AREQ_AddRequestFlags(req, QEXEC_F_SEND_HIGHLIGHT);

    } else if (AC_AdvanceIfMatch(ac, "HIGHLIGHT")) {
      if(!ensureSimpleMode(req)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "HIGHLIGHT is not supported on FT.AGGREGATE");
        return REDISMODULE_ERR;
      }

      if (ParseHighlight(ac, &req->outFields) == REDISMODULE_ERR) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for HIGHLIGHT");
        return REDISMODULE_ERR;
      }
      AREQ_AddRequestFlags(req, QEXEC_F_SEND_HIGHLIGHT);

    } else if ((AREQ_RequestFlags(req) & QEXEC_F_IS_SEARCH) &&
               ((rv = parseQueryLegacyArgs(ac, searchOpts, &hasEmptyFilterValue, status)) != ARG_UNKNOWN)) {
      if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      }
    } else if (AC_AdvanceIfMatch(ac, "WITHCOUNT")) {
      AREQ_RemoveRequestFlags(req, QEXEC_OPTIMIZE);
      optimization_specified = true;
    } else if (AC_AdvanceIfMatch(ac, "WITHOUTCOUNT")) {
      AREQ_AddRequestFlags(req, QEXEC_OPTIMIZE);
      optimization_specified = true;
    } else {
      ParseAggPlanContext papCtx = {
        .plan = AREQ_AGGPlan(req),
        .reqflags = &req->reqflags,
        .reqConfig = &req->reqConfig,
        .searchopts = &req->searchopts,
        .prefixesOffset = &req->prefixesOffset,
        .cursorConfig = &req->cursorConfig,
        .requiredFields = &req->requiredFields,
        .maxSearchResults = &req->maxSearchResults,
        .maxAggregateResults = &req->maxAggregateResults,
        .querySlots = &req->querySlots,
        .slotsVersion = &req->slotsVersion,
      };
      int rv = handleCommonArgs(&papCtx, ac, status);
      if (rv == ARG_HANDLED) {
        // nothing
      } else if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      } else {
        break;
      }
    }
  }

  // In dialect 2, we require a non empty numeric filter
  if (req->reqConfig.dialectVersion >= 2 && hasEmptyFilterValue){
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Numeric/Geo filter value/s cannot be empty");
      return REDISMODULE_ERR;
  }

  if (!optimization_specified && req->reqConfig.dialectVersion >= 4) {
    // If optimize was not enabled/disabled explicitly, enable it by default starting with dialect 4
    AREQ_AddRequestFlags(req, QEXEC_OPTIMIZE);
  }

  QEFlags reqFlags = AREQ_RequestFlags(req);
  if ((reqFlags & QEXEC_F_SEND_SCOREEXPLAIN) && !(reqFlags & QEXEC_F_SEND_SCORES)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "EXPLAINSCORE must be accompanied with WITHSCORES");
    return REDISMODULE_ERR;
  }

  if (IsSearch(req) && HasScoreInPipeline(req)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "ADDSCORES is not supported on FT.SEARCH");
    return REDISMODULE_ERR;
  }

  searchOpts->inkeys = (const sds*)inKeys.objs;
  searchOpts->ninkeys = inKeys.argc;
  searchOpts->legacy.infields = (const char **)inFields.objs;
  searchOpts->legacy.ninfields = inFields.argc;
  // if language is NULL, set it to RS_LANG_UNSET and it will be updated
  // later, taking the index language
  if(languageStr == NULL) {
    searchOpts->language = RS_LANG_UNSET;
  } else {
    searchOpts->language = RSLanguage_Find(languageStr, 0);
  }

  if (AC_IsInitialized(&returnFields)) {
    if(!ensureSimpleMode(req)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "RETURN is not supported on FT.AGGREGATE");
        return REDISMODULE_ERR;
    }

    req->outFields.explicitReturn = 1;
    if (returnFields.argc == 0) {
      AREQ_AddRequestFlags(req, QEXEC_F_SEND_NOFIELDS);
    }

    while (!AC_IsAtEnd(&returnFields)) {
      const char *path = AC_GetStringNC(&returnFields, NULL);
      const char *name = path;
      if (AC_AdvanceIfMatch(&returnFields, SPEC_AS_STR)) {
        int rv = AC_GetString(&returnFields, &name, NULL, 0);
        if (rv != AC_OK) {
          QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "RETURN path AS name - must be accompanied with NAME");
          return REDISMODULE_ERR;
        } else if (!strcasecmp(name, SPEC_AS_STR)) {
          QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Alias for RETURN cannot be `AS`");
          return REDISMODULE_ERR;
        }
      }
      ReturnedField *f = FieldList_GetCreateField(&req->outFields, name, path);
      f->explicitReturn = 1;
    }
    FieldList_RestrictReturn(&req->outFields);
  }
  return REDISMODULE_OK;
}

static char *getReducerAlias(PLN_GroupStep *g, const char *func, const ArgsCursor *args) {

  sds out = sdsnew("__generated_alias");
  out = sdscat(out, func);
  // only put parentheses if we actually have args
  char buf[255];
  ArgsCursor tmp = *args;
  while (!AC_IsAtEnd(&tmp)) {
    size_t l;
    const char *s = AC_GetStringNC(&tmp, &l);
    while (*s == '@') {
      // Don't allow the leading '@' to be included as an alias!
      ++s;
      --l;
    }
    out = sdscatlen(out, s, l);
    if (!AC_IsAtEnd(&tmp)) {
      out = sdscat(out, ",");
    }
  }

  // only put parentheses if we actually have args
  sdstolower(out);

  // duplicate everything. yeah this is lame but this function is not in a tight loop
  char *dup = rm_strndup(out, sdslen(out));
  sdsfree(out);
  return dup;
}

static void groupStepFree(PLN_BaseStep *base) {
  PLN_GroupStep *g = (PLN_GroupStep *)base;
  if (g->reducers) {
    size_t nreducers = array_len(g->reducers);
    for (size_t ii = 0; ii < nreducers; ++ii) {
      PLN_Reducer *gr = g->reducers + ii;
      rm_free(gr->alias);
    }
    array_free(g->reducers);
  }

  StrongRef_Release(g->properties_ref);

  RLookup_Cleanup(&g->lookup);
  rm_free(base);
}

static RLookup *groupStepGetLookup(PLN_BaseStep *bstp) {
  return &((PLN_GroupStep *)bstp)->lookup;
}


PLN_Reducer *PLNGroupStep_FindReducer(PLN_GroupStep *gstp, const char *name, ArgsCursor *ac) {
  long long nvars;
  if (AC_GetLongLong(ac, &nvars, 0) != AC_OK) {
    return NULL;
  }
  size_t nreducers = array_len(gstp->reducers);
  for (size_t ii = 0; ii < nreducers; ++ii) {
    PLN_Reducer *gr = gstp->reducers + ii;
    if (nvars == AC_NumArgs(&gr->args) && !strcasecmp(gr->name, name) && AC_Equals(ac, &gr->args)) {
      return gr;
    }
  }
  return NULL;
}

int PLNGroupStep_AddReducer(PLN_GroupStep *gstp, const char *name, ArgsCursor *ac,
                            QueryError *status) {
  // Just a list of functions..
  PLN_Reducer *gr = array_ensure_tail(&gstp->reducers, PLN_Reducer);

  gr->name = name;
  int rv = AC_GetVarArgs(ac, &gr->args);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, name, rv);
    goto error;
  }

  const char *alias = NULL;
  // See if there is an alias
  if (AC_AdvanceIfMatch(ac, "AS")) {
    rv = AC_GetString(ac, &alias, NULL, 0);
    if (rv != AC_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for AS: %s", AC_Strerror(rv));
      goto error;
    }
  }
  if (alias == NULL) {
    gr->alias = getReducerAlias(gstp, name, &gr->args);
  } else {
    gr->alias = rm_strdup(alias);
  }
  gr->isHidden = 0; // By default, reducers are not hidden
  return REDISMODULE_OK;

error:
  array_pop(gstp->reducers);
  return REDISMODULE_ERR;
}

static void genericStepFree(PLN_BaseStep *p) {
  rm_free(p);
}

// Helper function to get properties from StrongRef
arrayof(const char*) PLNGroupStep_GetProperties(const PLN_GroupStep *gstp) {
  return (arrayof(const char*))StrongRef_Get(gstp->properties_ref);
}

PLN_GroupStep *PLNGroupStep_New(StrongRef properties_ref, bool strictPrefix) {
  PLN_GroupStep *gstp = rm_calloc(1, sizeof(*gstp));
  gstp->properties_ref = properties_ref;
  gstp->base.dtor = groupStepFree;
  gstp->base.getLookup = groupStepGetLookup;
  gstp->base.type = PLN_T_GROUP;
  gstp->strictPrefix = strictPrefix;
  return gstp;
}

static int parseGroupby(AGGPlan *plan, ArgsCursor *ac, QueryError *status) {
  const char *s;
  AC_GetString(ac, &s, NULL, AC_F_NOADVANCE);

  long long nproperties;
  int rv = AC_GetLongLong(ac, &nproperties, 0);
  if (rv != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for GROUPBY: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }

  const char **properties = array_newlen(const char *, nproperties);
  for (size_t i = 0; i < nproperties; ++i) {
    const char *property;
    size_t propertyLen;
    rv = AC_GetString(ac, &property, &propertyLen, 0);
    if (rv != AC_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for GROUPBY: %s", AC_Strerror(rv));
      array_free(properties);
      return REDISMODULE_ERR;
    }
    if (property[0] != '@') {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for GROUPBY", ": Unknown property `%s`. Did you mean `@%s`?",
                         property, property);
      array_free(properties);
      return REDISMODULE_ERR;
    }
    properties[i] = property;
  }

  // Number of fields.. now let's see the reducers
  StrongRef properties_ref = StrongRef_New((void *)properties, (RefManager_Free)array_free);
  PLN_GroupStep *gstp = PLNGroupStep_New(properties_ref, false);
  AGPLN_AddStep(plan, &gstp->base);

  while (AC_AdvanceIfMatch(ac, "REDUCE")) {
    const char *name;
    if (AC_GetString(ac, &name, NULL, 0) != AC_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for REDUCE: %s", AC_Strerror(rv));
      return REDISMODULE_ERR;
    }
    if (PLNGroupStep_AddReducer(gstp, name, ac, status) != REDISMODULE_OK) {
      goto error;
    }
  }
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static void freeFilterStep(PLN_BaseStep *bstp) {
  PLN_MapFilterStep *fstp = (PLN_MapFilterStep *)bstp;
  if (fstp->parsedExpr) {
    ExprAST_Free(fstp->parsedExpr);
  }
  HiddenString_Free(fstp->expr, true);
  rm_free((void *)fstp->base.alias);
  rm_free(bstp);
}

PLN_MapFilterStep *PLNMapFilterStep_New(const HiddenString* expr, int mode) {
  PLN_MapFilterStep *stp = rm_calloc(1, sizeof(*stp));
  stp->base.dtor = freeFilterStep;
  stp->base.type = mode;
  stp->expr = HiddenString_Duplicate(expr);
  return stp;
}

static int handleApplyOrFilter(AGGPlan *plan, ArgsCursor *ac, QueryError *status, int isApply) {
  // Parse filters!
  const char *expr = NULL;
  size_t exprLen;
  int rv = AC_GetString(ac, &expr, &exprLen, 0);
  if (rv != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for APPLY/FILTER: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }

  HiddenString* expression = NewHiddenString(expr, exprLen, false);
  PLN_MapFilterStep *stp = PLNMapFilterStep_New(expression, isApply ? PLN_T_APPLY : PLN_T_FILTER);
  HiddenString_Free(expression, false);
  AGPLN_AddStep(plan, &stp->base);

  if (isApply) {
    if (AC_AdvanceIfMatch(ac, "AS")) {
      const char *alias;
      size_t aliasLen;
      if (AC_GetString(ac, &alias, &aliasLen, 0) != AC_OK) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "AS needs argument");
        goto error;
      }
      stp->base.alias = rm_strndup(alias, aliasLen);
    } else {
      stp->base.alias = rm_strndup(expr, exprLen);
    }
  }
  return REDISMODULE_OK;

error:
  if (stp) {
    AGPLN_PopStep(&stp->base);
    stp->base.dtor(&stp->base);
  }
  return REDISMODULE_ERR;
}

static int handleLoad(AGGPlan *plan, uint32_t *reqflags, ArgsCursor *ac, QueryError *status) {
  ArgsCursor loadfields = {0};
  int rc = AC_GetVarArgs(ac, &loadfields);
  if (rc == AC_ERR_PARSE) {
    // Didn't get a number, but we might have gotten a '*'
    const char *s = NULL;
    rc = AC_GetString(ac, &s, NULL, 0);
    if (rc != AC_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for LOAD: %s", AC_Strerror(rc));
      return REDISMODULE_ERR;
    } else if (strcmp(s, "*")) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for LOAD: Expected number of fields or `*`");
      return REDISMODULE_ERR;
    }
    // Successfully got a '*', load all fields
    REQFLAGS_AddFlags(reqflags, QEXEC_AGG_LOAD_ALL);
  } else if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for LOAD: %s", AC_Strerror(rc));
    return REDISMODULE_ERR;
  }

  PLN_LoadStep *lstp = rm_calloc(1, sizeof(*lstp));
  lstp->base.type = PLN_T_LOAD;
  lstp->base.dtor = loadDtor;
  if (loadfields.argc > 0) {
    lstp->args = loadfields;
    lstp->keys = rm_calloc(loadfields.argc, sizeof(*lstp->keys));
  }

  if (*reqflags & QEXEC_AGG_LOAD_ALL) {
    lstp->base.flags |= PLN_F_LOAD_ALL;
  }

  AGPLN_AddStep(plan, &lstp->base);
  return REDISMODULE_OK;
}

AREQ *AREQ_New(void) {
  AREQ* req = rm_calloc(1, sizeof(AREQ));
  /*
  unsigned int dialectVersion;
  long long queryTimeoutMS;
  RSTimeoutPolicy timeoutPolicy;
  int printProfileClock;
  uint64_t BM25STD_TanhFactor;
  */
  req->reqConfig = RSGlobalConfig.requestConfigParams;

  // TODO: save only one of the configuration parameters according to the query type
  // once query offset is bounded by both.
  req->maxSearchResults = RSGlobalConfig.maxSearchResults;
  req->maxAggregateResults = RSGlobalConfig.maxAggregateResults;
  req->optimizer = QOptimizer_New();
  req->profile = Profile_PrintDefault;
  req->prefixesOffset = 0;
  return req;
}

int parseAggPlan(ParseAggPlanContext *papCtx, ArgsCursor *ac, QueryError *status) {
  while (!AC_IsAtEnd(ac)) {
    int rv = handleCommonArgs(papCtx, ac, status);
    if (rv == ARG_HANDLED) {
      continue;
    } else if (rv == ARG_ERROR) {
      return REDISMODULE_ERR;
    }

    if (AC_AdvanceIfMatch(ac, "GROUPBY")) {
      if (!ensureExtendedMode(papCtx->reqflags, "GROUPBY", status)) {
        return REDISMODULE_ERR;
      }
      if (parseGroupby(papCtx->plan, ac, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (AC_AdvanceIfMatch(ac, "APPLY")) {
      if (handleApplyOrFilter(papCtx->plan, ac, status, 1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (AC_AdvanceIfMatch(ac, "LOAD")) {
      if (handleLoad(papCtx->plan, papCtx->reqflags, ac, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (AC_AdvanceIfMatch(ac, "FILTER")) {
      if (handleApplyOrFilter(papCtx->plan, ac, status, 0) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else {
      QueryError_FmtUnknownArg(status, ac, "<main>");
        return REDISMODULE_ERR;
    }
  }

  if (!(*papCtx->reqflags & QEXEC_F_SEND_HIGHLIGHT) &&
      !(*papCtx->reqflags & (QEXEC_F_SEND_SCORES | QEXEC_F_SEND_SCORES_AS_FIELD)) &&
      (!(*papCtx->reqflags & QEXEC_F_IS_SEARCH) || hasQuerySortby(papCtx->plan))) {
    // We can skip collecting full results structure and metadata from the iterators if:
    // 1. We don't have a highlight/summarize step,
    // 2. We are not required to return scores explicitly,
    // 3. This is not a search query with implicit sorting by query score.
    papCtx->searchopts->flags |= Search_CanSkipRichResults;
  }

  return REDISMODULE_OK;
}

int AREQ_Compile(AREQ *req, RedisModuleString **argv, int argc, QueryError *status) {
  req->args = rm_malloc(sizeof(*req->args) * argc);
  req->nargs = argc;
  // Copy the arguments into an owned array of sds strings
  for (size_t ii = 0; ii < argc; ++ii) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(argv[ii], &n);
    req->args[ii] = sdsnewlen(s, n);
  }

  // Parse the query and basic keywords first..
  ArgsCursor ac = {0};
  ArgsCursor_InitSDS(&ac, req->args, req->nargs);

  if (AC_IsAtEnd(&ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "No query string provided");
    return REDISMODULE_ERR;
  }

  req->query = AC_GetStringNC(&ac, NULL);
  initializeAREQ(req);
  RSSearchOptions *searchOpts = &req->searchopts;
  if (parseQueryArgs(&ac, req, searchOpts, &req->ast, AREQ_AGGPlan(req), status) != REDISMODULE_OK) {
    goto error;
  }

  // Now we have a 'compiled' plan. Let's get some more options..

  ParseAggPlanContext papCtx = {
    .plan = AREQ_AGGPlan(req),
    .reqflags = &req->reqflags,
    .reqConfig = &req->reqConfig,
    .searchopts = &req->searchopts,
    .prefixesOffset = &req->prefixesOffset,
    .cursorConfig = &req->cursorConfig,
    .requiredFields = &req->requiredFields,
    .maxSearchResults = &req->maxSearchResults,
    .maxAggregateResults = &req->maxAggregateResults,
    .querySlots = &req->querySlots,
    .slotsVersion = &req->slotsVersion,
  };
  if (parseAggPlan(&papCtx, &ac, status) != REDISMODULE_OK) {
    goto error;
  }

  // Verify we got slots requested if needed
  if (IsInternal(req) && !req->querySlots) {
    QueryError_SetError(status, QUERY_ERROR_CODE_MISSING, "Internal query missing slots specification");
    goto error;
  }

  // Define if we need a depleter in the pipeline
  if (IsAggregate(req) && !IsOptimized(req) && !IsCount(req)) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
    bool isLimited = (arng && arng->isLimited && arng->limit > 0);

    if (req->protocol == 2)
    {
      if (!HasSortBy(req) || (HasSortBy(req) && isLimited)) {
        // FT.AGGREGATE idx '*' WITHCOUNT + SORTBY + LIMIT
        // FT.AGGREGATE idx '*' WITHCOUNT + LIMIT
        AREQ_AddRequestFlags(req, QEXEC_F_HAS_DEPLETER);
      }
    } else if (req->protocol == 3) {
      if (isLimited) {
        // FT.AGGREGATE idx '*' WITHCOUNT + LIMIT
        // FT.AGGREGATE idx '*' WITHCOUNT + SORTBY + LIMIT
        AREQ_AddRequestFlags(req, QEXEC_F_HAS_DEPLETER);
      }
    }
  }

  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static int applyGlobalFilters(RSSearchOptions *opts, QueryAST *ast, const RedisSearchCtx *sctx, unsigned dialect, QueryError *status) {
  /** The following blocks will set filter options on the entire query */
  if (opts->legacy.filters) {
    for (size_t ii = 0; ii < array_len(opts->legacy.filters); ++ii) {
      LegacyNumericFilter *filter = opts->legacy.filters[ii];

      const FieldSpec *fs = IndexSpec_GetField(sctx->spec, filter->field);
      filter->base.fieldSpec = fs;
      if (!fs || !FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
        if (dialect != 1) {
          const HiddenString *fieldName = filter->field;
          if (fs) {
            QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Field is not a numeric field", ", field: %s", HiddenString_GetUnsafe(fieldName, NULL));
          } else {
            QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Unknown Field", " '%s'", HiddenString_GetUnsafe(fieldName, NULL));
          }
          return REDISMODULE_ERR;
        } else {
          // On DIALECT 1, we keep the legacy behavior of having an empty iterator when the field is invalid
          QAST_GlobalFilterOptions legacyFilterOpts = {.empty = true};
          QAST_SetGlobalFilters(ast, &legacyFilterOpts);
          continue; // Keep the filter entry in the legacy filters array for AREQ_Free()
        }
      }
      RS_ASSERT(filter->field);
      // Need to free the hidden string since we pass the base pointer to the query AST
      // And we are about to zero out the filter in the legacy filters
      HiddenString_Free(filter->field, false);
      filter->field = NULL;
      QAST_GlobalFilterOptions legacyFilterOpts = {.numeric = &filter->base};
      QAST_SetGlobalFilters(ast, &legacyFilterOpts);
      opts->legacy.filters[ii] = NULL;  // so AREQ_Free() doesn't free the filters themselves, which
                                        // are now owned by the query object
    }
  }
  if (opts->legacy.geo_filters) {
    for (size_t ii = 0; ii < array_len(opts->legacy.geo_filters); ++ii) {
      LegacyGeoFilter *gf = opts->legacy.geo_filters[ii];

      const FieldSpec *fs = IndexSpec_GetField(sctx->spec, gf->field);
      gf->base.fieldSpec = fs;
      if (!fs || !FIELD_IS(fs, INDEXFLD_T_GEO)) {
        if (dialect != 1) {
          const char *generalError = fs ? "Field is not a geo field" : "Unknown Field";
          QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, generalError, ", field: %s", HiddenString_GetUnsafe(gf->field, NULL));
          return REDISMODULE_ERR;
        } else {
          // On DIALECT 1, we keep the legacy behavior of having an empty iterator when the field is invalid
          QAST_GlobalFilterOptions legacyOpts = {.empty = true};
          QAST_SetGlobalFilters(ast, &legacyOpts);
        }
      } else {
        RS_ASSERT(gf->field);
        // Need to free the hidden string since we pass the base pointer to the query AST
        // And we are about to zero out the filter in the legacy filters
        HiddenString_Free(gf->field, false);
        gf->field = NULL;
        QAST_GlobalFilterOptions legacyOpts = {.geo = &gf->base};
        QAST_SetGlobalFilters(ast, &legacyOpts);
        opts->legacy.geo_filters[ii] = NULL;  // so AREQ_Free() doesn't free the filter itself, which is now owned
                                              // by the query object
      }
    }
  }

  if (opts->inkeys) {
    QAST_GlobalFilterOptions filterOpts = {.keys = opts->inkeys, .nkeys = opts->ninkeys};
    QAST_SetGlobalFilters(ast, &filterOpts);
  }
  return REDISMODULE_OK;
}

static bool IsIndexCoherent(AREQ *req) {
  if (req->prefixesOffset == 0) {
    // No prefixes in the query --> No validation needed.
    return true;
  }

  sds *args = req->args;
  long long n_prefixes = strtol(args[req->prefixesOffset + 1], NULL, 10);
  // The first argument is at req->prefixesOffset + 2
  sds *prefixes = args + req->prefixesOffset + 2;
  return IndexSpec_IsCoherent(AREQ_SearchCtx(req)->spec, prefixes, n_prefixes);
}


static int applyVectorQuery(AREQ *req, RedisSearchCtx *sctx, QueryAST *ast, QueryError *status) {
  ParsedVectorData *pvd = req->parsedVectorData;
  VectorQuery *vq = pvd->query;
  const char *fieldName = pvd->fieldName;

  // Resolve field spec
  const FieldSpec *vectorField = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, strlen(fieldName));
  if (!vectorField) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Unknown field", " `%s`", fieldName);
    return REDISMODULE_ERR;
  } else if (!FIELD_IS(vectorField, INDEXFLD_T_VECTOR)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Expected a " SPEC_VECTOR_STR " field", " `%s`", fieldName);
    return REDISMODULE_ERR;
  }
  vq->field = vectorField;

  QueryNode *vecNode = NewQueryNode(QN_VECTOR);
  vecNode->vn.vq = vq;
  pvd->query = NULL;
  //QueryNode now owns the VectorQuery

  // Apply the flags that were set during parsing
  vecNode->opts.flags |= pvd->queryNodeFlags;

  if (pvd->isParameter) {
    // PARAMETER CASE: Set up parameter for evalnode to resolve later
    QueryToken vecToken = {
      .type = QT_PARAM_VEC,
      .s = (vq->type == VECSIM_QT_KNN) ? vq->knn.vector : vq->range.vector,
      .len = (vq->type == VECSIM_QT_KNN) ? vq->knn.vecLen : vq->range.vecLen,
      .pos = 0,
      .numval = 0,
      .sign = 0
    };

    QueryParseCtx q = {0};
    QueryNode_InitParams(vecNode, 1);
    switch (vq->type) {
      case VECSIM_QT_KNN:
        QueryNode_SetParam(&q, &vecNode->params[0], &vq->knn.vector, &vq->knn.vecLen, &vecToken);
        break;
      case VECSIM_QT_RANGE:
        QueryNode_SetParam(&q, &vecNode->params[0], &vq->range.vector, &vq->range.vecLen, &vecToken);
        break;
    }
    // Update AST's numParams since we used a local QueryParseCtx
    ast->numParams += q.numParams;
  }
  // Handle non-vector-specific attributes (like YIELD_SCORE_AS)
  if (pvd->attributes) {
    QueryNode_ApplyAttributes(vecNode, pvd->attributes, array_len(pvd->attributes), status);
  }

  // Set vector node as ast->root and use setFilterNode for proper filter integration
  // setFilterNode handles both KNN (child relationship) and RANGE (intersection) properly
  QueryNode *oldRoot = ast->root;
  ast->root = vecNode;
  SetFilterNode(ast, oldRoot);

  return REDISMODULE_OK;
}

int AREQ_ApplyContext(AREQ *req, RedisSearchCtx *sctx, QueryError *status) {
  // Sort through the applicable options:
  IndexSpec *index = sctx->spec;
  RSSearchOptions *opts = &req->searchopts;
  req->sctx = sctx;

  if (!IsIndexCoherent(req)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_MISMATCH, NULL);
    return REDISMODULE_ERR;
  }

  QEFlags reqFlags = AREQ_RequestFlags(req);
  if (isSpecJson(index) && (reqFlags & QEXEC_F_SEND_HIGHLIGHT)) {
    QueryError_SetError(
        status, QUERY_ERROR_CODE_INVAL,
        "HIGHLIGHT/SUMMARIZE is not supported with JSON indexes");
    return REDISMODULE_ERR;
  }

  if ((index->flags & Index_StoreByteOffsets) == 0 && (reqFlags & QEXEC_F_SEND_HIGHLIGHT)) {
    QueryError_SetError(
        status, QUERY_ERROR_CODE_INVAL,
        "Cannot use HIGHLIGHT/SUMMARIZE because NOOFSETS was specified at index level");
    return REDISMODULE_ERR;
  }

  // Go through the query options and see what else needs to be filled in!
  // 1) INFIELDS
  if (opts->legacy.ninfields) {
    opts->fieldmask = 0;
    for (size_t ii = 0; ii < opts->legacy.ninfields; ++ii) {
      const char *s = opts->legacy.infields[ii];
      t_fieldMask bit = IndexSpec_GetFieldBit(index, s, strlen(s));
      opts->fieldmask |= bit;
    }
  }

  if (opts->language == RS_LANG_UNSET) {
    opts->language = index->rule->lang_default;
  } else if (opts->language == RS_LANG_UNSUPPORTED) {
    QueryError_SetError(status, QUERY_ERROR_CODE_INVAL, "No such language");
    return REDISMODULE_ERR;
  }

  if (opts->scorerName) {
    if (Extensions_GetScoringFunction(NULL, opts->scorerName) == NULL) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "No such scorer %s", opts->scorerName);
      return REDISMODULE_ERR;
    }
  } else {
    opts->scorerName = RSGlobalConfig.defaultScorer;
  }

  bool resp3 = req->protocol == 3;
  if (SetValueFormat(resp3, isSpecJson(index), &req->reqflags, status) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (!(opts->flags & Search_NoStopWords)) {
    opts->stopwords = sctx->spec->stopwords;
    StopWordList_Ref(sctx->spec->stopwords);
  }

  req->slotRanges = Slots_GetLocalSlots();

  SetSearchCtx(sctx, req);
  QueryAST *ast = &req->ast;

  unsigned long dialectVersion = req->reqConfig.dialectVersion;

  int rv = QAST_Parse(ast, sctx, opts, req->query, strlen(req->query), dialectVersion, status);
  if (rv != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (req->parsedVectorData) {
    int rv = applyVectorQuery(req, sctx, ast, status);
    if (rv != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  if (QAST_EvalParams(ast, opts, dialectVersion, status) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (applyGlobalFilters(opts, ast, sctx, dialectVersion, status) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (QAST_CheckIsValid(ast, AREQ_SearchCtx(req)->spec, opts, status) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (!(opts->flags & Search_Verbatim)) {
    if (QAST_Expand(ast, opts->expanderName, opts, sctx, status) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  // set queryAST configuration parameters
  iteratorsConfig_init(&ast->config);

  if (IsOptimized(req)) {
    // parse inputs for optimizations
    QOptimizer_Parse(req);
    // check possible optimization after creation of QueryNode tree
    QOptimizer_QueryNodes(req->ast.root, req->optimizer);
  }

  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void AREQ_Free(AREQ *req) {
  // Check if rootiter exists but pipeline was never built (no result processors)
  // In this case, we need to free the rootiter manually since no RPQueryIterator
  // was created to take ownership of it.
  bool rootiterNeedsFreeing = (req->rootiter != NULL && req->pipeline.qctx.rootProc == NULL);

  // First, free the pipeline
  Pipeline_Clean(&req->pipeline);

  // Free the rootiter if it wasn't transferred to the pipeline.
  // The RPQueryIterator takes ownership of rootiter when the pipeline is built,
  // but in cases like RS_GetExplainOutput or pipeline build failures,
  // the rootiter may exist without being owned by any result processor.
  if (rootiterNeedsFreeing) {
    req->rootiter->Free(req->rootiter);
  }
  req->rootiter = NULL;
  if (req->optimizer) {
    QOptimizer_Free(req->optimizer);
  }

  QAST_Destroy(&req->ast);

  if (req->searchopts.stopwords) {
    StopWordList_Unref((StopWordList *)req->searchopts.stopwords);
  }

  Slots_FreeLocalSlots(req->slotRanges);
  rm_free((void *)req->querySlots);

  // Finally, free the context. If we are a cursor or have multi workers threads,
  // we need also to detach the ("Thread Safe") context.
  RedisModuleCtx *thctx = NULL;
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  if (sctx) {
    if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      thctx = sctx->redisCtx;
      sctx->redisCtx = NULL;
    }
    // Here we unlock the spec
    SearchCtx_Free(sctx);
  }

  for (size_t ii = 0; ii < req->nargs; ++ii) {
    sdsfree(req->args[ii]);
    req->args[ii] = NULL;
  }
  if (req->searchopts.legacy.filters) {
    for (size_t ii = 0; ii < array_len(req->searchopts.legacy.filters); ++ii) {
      LegacyNumericFilter *nf = req->searchopts.legacy.filters[ii];
      if (nf) {
        LegacyNumericFilter_Free(req->searchopts.legacy.filters[ii]);
      }
    }
    array_free(req->searchopts.legacy.filters);
  }
  if (req->searchopts.legacy.geo_filters) {
    array_foreach(req->searchopts.legacy.geo_filters, gf, if (gf) LegacyGeoFilter_Free(gf));
    array_free(req->searchopts.legacy.geo_filters);
  }
  if (req->searchopts.params) {
    Param_DictFree(req->searchopts.params);
  }
  FieldList_Free(&req->outFields);
  if (thctx) {
    RedisModule_FreeThreadSafeContext(thctx);
  }
  if(req->requiredFields) {
    array_free(req->requiredFields);
  }
  // Free parsed vector data
  if (req->parsedVectorData) {
    ParsedVectorData_Free(req->parsedVectorData);
    req->parsedVectorData = NULL;
  }

  rm_free(req->args);
  rm_free(req);
}



int AREQ_BuildPipeline(AREQ *req, QueryError *status) {
  Pipeline_Initialize(&req->pipeline, req->reqConfig.timeoutPolicy, status);
  if (!(AREQ_RequestFlags(req) & QEXEC_F_BUILDPIPELINE_NO_ROOT)) {
    QueryPipelineParams params = {
      .common = {
        .sctx = req->sctx,
        .reqflags = req->reqflags,
        .optimizer = req->optimizer,
        .scoreAlias = req->searchopts.scoreAlias,
      },
      .ast = &req->ast,
      .rootiter = req->rootiter,
      .slotRanges = req->slotRanges,
      .scorerName = req->searchopts.scorerName,
      .reqConfig = &req->reqConfig,
    };
    req->rootiter = NULL; // Ownership of the root iterator is now with the params.
    req->slotRanges = NULL; // Ownership of the slot ranges is now with the params.
    Pipeline_BuildQueryPart(&req->pipeline, &params);
    if (QueryError_HasError(status)) {
      return REDISMODULE_ERR;
    }
  }
  AggregationPipelineParams params = {
    .common = {
      .sctx = req->sctx,
      .reqflags = req->reqflags,
      .optimizer = req->optimizer,
      // Right now score alias is not supposed to be used in the aggregation pipeline
      .scoreAlias = NULL,
    },
    .outFields = &req->outFields,
    .maxResultsLimit = IsSearch(req) ? req->maxSearchResults : req->maxAggregateResults,
    .language = req->searchopts.language,
  };
  return Pipeline_BuildAggregationPart(&req->pipeline, &params, &req->stateflags);
}
