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

/**
 * Ensures that the user has not requested one of the 'extended' features. Extended
 * in this case refers to reducers which re-create the search results.
 * @param areq the request
 * @param name the name of the option that requires simple mode. Used for error
 *   formatting
 * @param status the error object
 */
static void ensureSimpleMode(AREQ *areq) {
  RS_LOG_ASSERT(!(areq->reqflags & QEXEC_F_IS_EXTENDED), "Single mod test failed");
  areq->reqflags |= QEXEC_F_IS_SEARCH;
}

/**
 * Like @ref ensureSimpleMode(), but does the opposite -- ensures that one of the
 * 'simple' options - i.e. ones which rely on the field to be the exact same as
 * found in the document - was not requested.
 */
static int ensureExtendedMode(AREQ *areq, const char *name, QueryError *status) {
  if (areq->reqflags & QEXEC_F_IS_SEARCH) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL,
                           "option `%s` is mutually exclusive with simple (i.e. search) options",
                           name);
    return 0;
  }
  areq->reqflags |= QEXEC_F_IS_EXTENDED;
  return 1;
}

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, int allowLegacy);

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
  ret->path = path;
  ret->name = (name) ? name : path;
  return ret;
}

static void FieldList_RestrictReturn(FieldList *fields) {
  if (!fields->explicitReturn) {
    return;
  }

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

static int parseCursorSettings(AREQ *req, ArgsCursor *ac, QueryError *status) {
  ACArgSpec specs[] = {{.name = "MAXIDLE",
                        .type = AC_ARGTYPE_UINT,
                        .target = &req->cursorMaxIdle,
                        .intflags = AC_F_GE1},
                       {.name = "COUNT",
                        .type = AC_ARGTYPE_UINT,
                        .target = &req->cursorChunkSize,
                        .intflags = AC_F_GE1},
                       {NULL}};

  int rv;
  ACArgSpec *errArg = NULL;
  if ((rv = AC_ParseArgSpec(ac, specs, &errArg)) != AC_OK && rv != AC_ERR_ENOENT) {
    QERR_MKBADARGS_AC(status, errArg->name, rv);
    return REDISMODULE_ERR;
  }

  if (req->cursorMaxIdle == 0 || req->cursorMaxIdle > RSGlobalConfig.cursorMaxIdle) {
    req->cursorMaxIdle = RSGlobalConfig.cursorMaxIdle;
  }
  req->reqflags |= QEXEC_F_IS_CURSOR;
  return REDISMODULE_OK;
}

#define ARG_HANDLED 1
#define ARG_ERROR -1
#define ARG_UNKNOWN 0

static int handleCommonArgs(AREQ *req, ArgsCursor *ac, QueryError *status, int allowLegacy) {
  int rv;
  // This handles the common arguments that are not stateful
  if (AC_AdvanceIfMatch(ac, "LIMIT")) {
    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(&req->ap);
    arng->isLimited = 1;
    // Parse offset, length
    if (AC_NumRemaining(ac) < 2) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT requires two arguments");
      return ARG_ERROR;
    }
    if ((rv = AC_GetU64(ac, &arng->offset, 0)) != AC_OK ||
        (rv = AC_GetU64(ac, &arng->limit, 0)) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT needs two numeric arguments");
      return ARG_ERROR;
    }

    if (arng->isLimited && arng->limit == 0) {
      // LIMIT 0 0 - only count
      req->reqflags |= QEXEC_F_NOROWS;
      req->reqflags |= QEXEC_F_SEND_NOFIELDS;
    } else if ((arng->limit > RSGlobalConfig.maxSearchResults) &&
               (req->reqflags & QEXEC_F_IS_SEARCH)) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "LIMIT exceeds maximum of %llu",
                             RSGlobalConfig.maxSearchResults);
      return ARG_ERROR;
    } else if ((arng->limit > RSGlobalConfig.maxAggregateResults) &&
               !(req->reqflags & QEXEC_F_IS_SEARCH)) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "LIMIT exceeds maximum of %llu",
                             RSGlobalConfig.maxAggregateResults);
      return ARG_ERROR;
    } else if (arng->offset > RSGlobalConfig.maxSearchResults) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "OFFSET exceeds maximum of %llu",
                             RSGlobalConfig.maxSearchResults);
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "SORTBY")) {
    PLN_ArrangeStep *arng = AGPLN_GetOrCreateArrangeStep(&req->ap);
    if ((parseSortby(arng, ac, status, req->reqflags & QEXEC_F_IS_SEARCH)) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "TIMEOUT")) {	
    if (AC_NumRemaining(ac) < 1) {	
      QueryError_SetError(status, QUERY_EPARSEARGS, "Need argument for TIMEOUT");	
      return ARG_ERROR;	
    }	
    if (AC_GetInt(ac, &req->reqTimeout, AC_F_GE0) != AC_OK) {	
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "TIMEOUT requires a non negative integer");	
      return ARG_ERROR;	
    }
  } else if (AC_AdvanceIfMatch(ac, "WITHCURSOR")) {
    if (parseCursorSettings(req, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "_NUM_SSTRING")) {
    req->reqflags |= QEXEC_F_TYPED;
  } else if (AC_AdvanceIfMatch(ac, "WITHRAWIDS")) {
    req->reqflags |= QEXEC_F_SENDRAWIDS;
  } else {
    return ARG_UNKNOWN;
  }

  return ARG_HANDLED;
}

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, int isLegacy) {
  // Prevent multiple SORTBY steps
  if (arng->sortKeys != NULL) {
    QERR_MKBADARGS_FMT(status, "Multiple SORTBY steps are not allowed. Sort multiple fields in a single step");
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
      QERR_MKBADARGS_AC(status, "SORTBY", rv);
      goto err;
    }
  }

  keys = array_new(const char *, 8);

  if (isLegacy) {
    // Legacy demands one field and an optional ASC/DESC parameter. Both
    // of these are handled above, so no need for argument parsing
    const char *s = AC_GetStringNC(&subArgs, NULL);
    keys = array_append(keys, s);

    if (legacyDesc) {
      SORTASCMAP_SETDESC(ascMap, 0);
    }
  } else {
    while (!AC_IsAtEnd(&subArgs)) {

      const char *s = AC_GetStringNC(&subArgs, NULL);
      if (*s == '@') {
        if (array_len(keys) >= SORTASCMAP_MAXFIELDS) {
          QERR_MKBADARGS_FMT(status, "Cannot sort by more than %lu fields", SORTASCMAP_MAXFIELDS);
          goto err;
        }
        s++;
        keys = array_append(keys, s);
        continue;
      }

      if (!strcasecmp(s, "ASC")) {
        SORTASCMAP_SETASC(ascMap, array_len(keys) - 1);
      } else if (!strcasecmp(s, "DESC")) {
        SORTASCMAP_SETDESC(ascMap, array_len(keys) - 1);
      } else {
        // Unknown token - neither a property nor ASC/DESC
        QERR_MKBADARGS_FMT(status, "MISSING ASC or DESC after sort field (%s)", s);
        goto err;
      }
    }
  }

  // Parse optional MAX
  // MAX is not included in the normal SORTBY arglist.. so we need to switch
  // back to `ac`
  if (AC_AdvanceIfMatch(ac, "MAX")) {
    unsigned mx = 0;
    if ((rv = AC_GetUnsigned(ac, &mx, 0) != AC_OK)) {
      QERR_MKBADARGS_AC(status, "MAX", rv);
      goto err;
    }
    arng->limit = mx;
  }

  arng->sortAscMap = ascMap;
  arng->sortKeys = keys;
  return REDISMODULE_OK;
err:
  QERR_MKBADARGS_FMT(status, "Bad SORTBY arguments");
  if (keys) {
    array_free(keys);
  }
  return REDISMODULE_ERR;
}

static int parseQueryLegacyArgs(ArgsCursor *ac, RSSearchOptions *options, QueryError *status) {
  if (AC_AdvanceIfMatch(ac, "FILTER")) {
    // Numeric filter
    NumericFilter **curpp = array_ensure_tail(&options->legacy.filters, NumericFilter *);
    *curpp = NumericFilter_Parse(ac, status);
    if (!*curpp) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "GEOFILTER")) {
    options->legacy.gf = rm_calloc(1, sizeof(*options->legacy.gf));
    if (GeoFilter_Parse(options->legacy.gf, ac, status) != REDISMODULE_OK) {
      GeoFilter_Free(options->legacy.gf);
      return ARG_ERROR;
    }
  } else {
    return ARG_UNKNOWN;
  }
  return ARG_HANDLED;
}

static int parseQueryArgs(ArgsCursor *ac, AREQ *req, RSSearchOptions *searchOpts,
                          AggregatePlan *plan, QueryError *status) {
  // Parse query-specific arguments..
  const char *languageStr = NULL;
  ArgsCursor returnFields = {0};
  ArgsCursor inKeys = {0};
  ArgsCursor inFields = {0};
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
      {AC_MKBITFLAG("WITHSORTKEYS", &req->reqflags, QEXEC_F_SEND_SORTKEYS)},
      {AC_MKBITFLAG("WITHPAYLOADS", &req->reqflags, QEXEC_F_SEND_PAYLOADS)},
      {AC_MKBITFLAG("NOCONTENT", &req->reqflags, QEXEC_F_SEND_NOFIELDS)},
      {AC_MKBITFLAG("NOSTOPWORDS", &searchOpts->flags, Search_NoStopwrods)},
      {AC_MKBITFLAG("EXPLAINSCORE", &req->reqflags, QEXEC_F_SEND_SCOREEXPLAIN)},
      {.name = "PAYLOAD",
       .type = AC_ARGTYPE_STRING,
       .target = &req->ast.udata,
       .len = &req->ast.udatalen},
      {NULL}};

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
      ensureSimpleMode(req);
      if (ParseSummarize(ac, &req->outFields) == REDISMODULE_ERR) {
        QERR_MKBADARGS_FMT(status, "Bad arguments for SUMMARIZE");
        return REDISMODULE_ERR;
      }
      req->reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if (AC_AdvanceIfMatch(ac, "HIGHLIGHT")) {
      ensureSimpleMode(req);
      if (ParseHighlight(ac, &req->outFields) == REDISMODULE_ERR) {
        QERR_MKBADARGS_FMT(status, "Bad arguments for HIGHLIGHT");
        return REDISMODULE_ERR;
      }
      req->reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if ((req->reqflags & QEXEC_F_IS_SEARCH) &&
               ((rv = parseQueryLegacyArgs(ac, searchOpts, status)) != ARG_UNKNOWN)) {
      if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      }
    } else {
      int rv = handleCommonArgs(req, ac, status, 1);
      if (rv == ARG_HANDLED) {
        // nothing
      } else if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      } else {
        break;
      }
    }
  }

  if ((req->reqflags & QEXEC_F_SEND_SCOREEXPLAIN) && !(req->reqflags & QEXEC_F_SEND_SCORES)) {
    QERR_MKBADARGS_FMT(status, "EXPLAINSCORE must be accompanied with WITHSCORES");
    return REDISMODULE_ERR;
  }

  searchOpts->inkeys = (const char **)inKeys.objs;
  searchOpts->ninkeys = inKeys.argc;
  searchOpts->legacy.infields = (const char **)inFields.objs;
  searchOpts->legacy.ninfields = inFields.argc;
  searchOpts->language = RSLanguage_Find(languageStr, 0);

  if (AC_IsInitialized(&returnFields)) {
    ensureSimpleMode(req);

    req->outFields.explicitReturn = 1;
    if (returnFields.argc == 0) {
      req->reqflags |= QEXEC_F_SEND_NOFIELDS;
    }

    while (!AC_IsAtEnd(&returnFields)) {
      const char *path = AC_GetStringNC(&returnFields, NULL);
      const char *name = path;
      if (AC_AdvanceIfMatch(&returnFields, SPEC_AS_STR)) {
        int rv = AC_GetString(&returnFields, &name, NULL, 0);
        if (rv != AC_OK) {
          QERR_MKBADARGS_FMT(status, "RETURN path AS name - must be accompanied with NAME");
          return REDISMODULE_ERR;
        } else if (!strncasecmp(name, SPEC_AS_STR, strlen(SPEC_AS_STR))) {
          QERR_MKBADARGS_FMT(status, "Alias for RETURN cannot be `AS`");
          return REDISMODULE_ERR;
        }
      }
      ReturnedField *f = FieldList_GetCreateField(&req->outFields, name, path);
      f->explicitReturn = 1;
    }
  }

  FieldList_RestrictReturn(&req->outFields);
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

  RLookup_Cleanup(&g->lookup);
  rm_free(base);
}

static RLookup *groupStepGetLookup(PLN_BaseStep *bstp) {
  return &((PLN_GroupStep *)bstp)->lookup;
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
      QERR_MKBADARGS_AC(status, "AS", rv);
      goto error;
    }
  }
  if (alias == NULL) {
    gr->alias = getReducerAlias(gstp, name, &gr->args);
  } else {
    gr->alias = rm_strdup(alias);
  }
  return REDISMODULE_OK;

error:
  array_pop(gstp->reducers);
  return REDISMODULE_ERR;
}

static void genericStepFree(PLN_BaseStep *p) {
  rm_free(p);
}

PLN_GroupStep *PLNGroupStep_New(const char **properties, size_t nproperties) {
  PLN_GroupStep *gstp = rm_calloc(1, sizeof(*gstp));
  gstp->properties = properties;
  gstp->nproperties = nproperties;
  gstp->base.dtor = groupStepFree;
  gstp->base.getLookup = groupStepGetLookup;
  gstp->base.type = PLN_T_GROUP;
  return gstp;
}

static int parseGroupby(AREQ *req, ArgsCursor *ac, QueryError *status) {
  ArgsCursor groupArgs = {0};
  const char *s;
  AC_GetString(ac, &s, NULL, AC_F_NOADVANCE);
  int rv = AC_GetVarArgs(ac, &groupArgs);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "GROUPBY", rv);
    return REDISMODULE_ERR;
  }

  // Number of fields.. now let's see the reducers
  PLN_GroupStep *gstp = PLNGroupStep_New((const char **)groupArgs.objs, groupArgs.argc);
  AGPLN_AddStep(&req->ap, &gstp->base);

  while (AC_AdvanceIfMatch(ac, "REDUCE")) {
    const char *name;
    if (AC_GetString(ac, &name, NULL, 0) != AC_OK) {
      QERR_MKBADARGS_AC(status, "REDUCE", rv);
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
  if (fstp->shouldFreeRaw) {
    rm_free((char *)fstp->rawExpr);
  }
  rm_free((void *)fstp->base.alias);
  rm_free(bstp);
}

PLN_MapFilterStep *PLNMapFilterStep_New(const char *expr, int mode) {
  PLN_MapFilterStep *stp = rm_calloc(1, sizeof(*stp));
  stp->base.dtor = freeFilterStep;
  stp->base.type = mode;
  stp->rawExpr = expr;
  return stp;
}

static int handleApplyOrFilter(AREQ *req, ArgsCursor *ac, QueryError *status, int isApply) {
  // Parse filters!
  const char *expr = NULL;
  int rv = AC_GetString(ac, &expr, NULL, 0);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "APPLY/FILTER", rv);
    return REDISMODULE_ERR;
  }

  PLN_MapFilterStep *stp = PLNMapFilterStep_New(expr, isApply ? PLN_T_APPLY : PLN_T_FILTER);
  AGPLN_AddStep(&req->ap, &stp->base);

  if (isApply) {
    if (AC_AdvanceIfMatch(ac, "AS")) {
      const char *alias;
      if (AC_GetString(ac, &alias, NULL, 0) != AC_OK) {
        QERR_MKBADARGS_FMT(status, "AS needs argument");
        goto error;
      }
      stp->base.alias = rm_strdup(alias);
    } else {
      stp->base.alias = rm_strdup(expr);
    }
  }
  return REDISMODULE_OK;

error:
  if (stp) {
    AGPLN_PopStep(&req->ap, &stp->base);
    stp->base.dtor(&stp->base);
  }
  return REDISMODULE_ERR;
}

static void loadDtor(PLN_BaseStep *bstp) {
  PLN_LoadStep *lstp = (PLN_LoadStep *)bstp;
  rm_free(lstp->keys);
  rm_free(lstp);
}

static int handleLoad(AREQ *req, ArgsCursor *ac, QueryError *status) {
  ArgsCursor loadfields = {0};
  int rc = AC_GetVarArgs(ac, &loadfields);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(status, "LOAD", rc);
    return REDISMODULE_ERR;
  }
  PLN_LoadStep *lstp = rm_calloc(1, sizeof(*lstp));
  lstp->base.type = PLN_T_LOAD;
  lstp->base.dtor = loadDtor;
  lstp->args = loadfields;
  lstp->keys = rm_calloc(loadfields.argc, sizeof(*lstp->keys));

  AGPLN_AddStep(&req->ap, &lstp->base);
  return REDISMODULE_OK;
}

AREQ *AREQ_New(void) {
  return rm_calloc(1, sizeof(AREQ));
}

int AREQ_Compile(AREQ *req, RedisModuleString **argv, int argc, QueryError *status) {
  req->args = rm_malloc(sizeof(*req->args) * argc);
  req->nargs = argc;
  for (size_t ii = 0; ii < argc; ++ii) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(argv[ii], &n);
    req->args[ii] = sdsnewlen(s, n);
  }

  // Parse the query and basic keywords first..
  ArgsCursor ac = {0};
  ArgsCursor_InitSDS(&ac, req->args, req->nargs);

  if (AC_IsAtEnd(&ac)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "No query string provided");
    return REDISMODULE_ERR;
  }

  req->query = AC_GetStringNC(&ac, NULL);
  AGPLN_Init(&req->ap);

  RSSearchOptions *searchOpts = &req->searchopts;
  RSSearchOptions_Init(searchOpts);
  if (parseQueryArgs(&ac, req, searchOpts, &req->ap, status) != REDISMODULE_OK) {
    goto error;
  }

  int hasLoad = 0;

  // Now we have a 'compiled' plan. Let's get some more options..

  while (!AC_IsAtEnd(&ac)) {
    int rv = handleCommonArgs(req, &ac, status, req->reqflags & QEXEC_F_IS_SEARCH);
    if (rv == ARG_HANDLED) {
      continue;
    } else if (rv == ARG_ERROR) {
      goto error;
    }

    if (AC_AdvanceIfMatch(&ac, "GROUPBY")) {
      if (!ensureExtendedMode(req, "GROUPBY", status)) {
        goto error;
      }
      if (parseGroupby(req, &ac, status) != REDISMODULE_OK) {
        goto error;
      }
    } else if (AC_AdvanceIfMatch(&ac, "APPLY")) {
      if (handleApplyOrFilter(req, &ac, status, 1) != REDISMODULE_OK) {
        goto error;
      }
    } else if (AC_AdvanceIfMatch(&ac, "LOAD")) {
      if (handleLoad(req, &ac, status) != REDISMODULE_OK) {
        goto error;
      }
    } else if (AC_AdvanceIfMatch(&ac, "FILTER")) {
      if (handleApplyOrFilter(req, &ac, status, 0) != REDISMODULE_OK) {
        goto error;
      }
    } else {
      QueryError_FmtUnknownArg(status, &ac, "<main>");
      goto error;
    }
  }
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static void applyGlobalFilters(RSSearchOptions *opts, QueryAST *ast, const RedisSearchCtx *sctx) {
  /** The following blocks will set filter options on the entire query */
  if (opts->legacy.filters) {
    for (size_t ii = 0; ii < array_len(opts->legacy.filters); ++ii) {
      QAST_GlobalFilterOptions legacyFilterOpts = {.numeric = opts->legacy.filters[ii]};
      QAST_SetGlobalFilters(ast, &legacyFilterOpts);
    }
    array_clear(opts->legacy.filters);  // so AREQ_Free() doesn't free the filters themselves, which
                                        // are now owned by the query object
  }
  if (opts->legacy.gf) {
    QAST_GlobalFilterOptions legacyOpts = {.geo = opts->legacy.gf};
    QAST_SetGlobalFilters(ast, &legacyOpts);
  }

  if (opts->inkeys) {
    opts->inids = rm_malloc(sizeof(*opts->inids) * opts->ninkeys);
    for (size_t ii = 0; ii < opts->ninkeys; ++ii) {
      t_docId did = DocTable_GetId(&sctx->spec->docs, opts->inkeys[ii], strlen(opts->inkeys[ii]));
      if (did) {
        opts->inids[opts->nids++] = did;
      }
    }
    QAST_GlobalFilterOptions filterOpts = {.ids = opts->inids, .nids = opts->nids};
    QAST_SetGlobalFilters(ast, &filterOpts);
  }
}

int AREQ_ApplyContext(AREQ *req, RedisSearchCtx *sctx, QueryError *status) {
  // Sort through the applicable options:
  IndexSpec *index = sctx->spec;
  RSSearchOptions *opts = &req->searchopts;
  req->sctx = sctx;

  if ((index->flags & Index_StoreByteOffsets) == 0 && (req->reqflags & QEXEC_F_SEND_HIGHLIGHT)) {
    QueryError_SetError(
        status, QUERY_EINVAL,
        "Cannot use highlight/summarize because NOOFSETS was specified at index level");
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

  if (opts->language == RS_LANG_UNSUPPORTED) {
    QueryError_SetError(status, QUERY_EINVAL, "No such language");
    return REDISMODULE_ERR;
  }
  if (opts->scorerName && Extensions_GetScoringFunction(NULL, opts->scorerName) == NULL) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL, "No such scorer %s", opts->scorerName);
    return REDISMODULE_ERR;
  }
  if (!(opts->flags & Search_NoStopwrods)) {
    opts->stopwords = sctx->spec->stopwords;
    StopWordList_Ref(sctx->spec->stopwords);
  }

  QueryAST *ast = &req->ast;

  int rv = QAST_Parse(ast, sctx, &req->searchopts, req->query, strlen(req->query), status);
  if (rv != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  applyGlobalFilters(opts, ast, sctx);

  if (!(opts->flags & Search_Verbatim)) {
    if (QAST_Expand(ast, opts->expanderName, opts, sctx, status) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  ConcurrentSearchCtx_Init(sctx->redisCtx, &req->conc);
  req->rootiter = QAST_Iterate(ast, opts, sctx, &req->conc);
  RS_LOG_ASSERT(req->rootiter, "QAST_Iterate failed");
  if (IsProfile(req)) {
    // Add a Profile iterators before every iterator in the tree
    Profile_AddIters(&req->rootiter);
  }

  return REDISMODULE_OK;
}

static ResultProcessor *buildGroupRP(PLN_GroupStep *gstp, RLookup *srclookup, QueryError *err) {
  const RLookupKey *srckeys[gstp->nproperties], *dstkeys[gstp->nproperties];
  for (size_t ii = 0; ii < gstp->nproperties; ++ii) {
    const char *fldname = gstp->properties[ii] + 1;  // account for the @-
    srckeys[ii] = RLookup_GetKey(srclookup, fldname, RLOOKUP_F_NOINCREF);
    if (!srckeys[ii]) {
      QueryError_SetErrorFmt(err, QUERY_ENOPROPKEY, "No such property `%s`", fldname);
      return NULL;
    }
    dstkeys[ii] = RLookup_GetKey(&gstp->lookup, fldname, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
  }

  Grouper *grp = Grouper_New(srckeys, dstkeys, gstp->nproperties);

  size_t nreducers = array_len(gstp->reducers);
  for (size_t ii = 0; ii < nreducers; ++ii) {
    // Build the actual reducer
    PLN_Reducer *pr = gstp->reducers + ii;
    ReducerOptions options = REDUCEROPTS_INIT(pr->name, &pr->args, srclookup, err);
    ReducerFactory ff = RDCR_GetFactory(pr->name);
    if (!ff) {
      // No such reducer!
      Grouper_Free(grp);
      QueryError_SetErrorFmt(err, QUERY_ENOREDUCER, "No such reducer: %s", pr->name);
      return NULL;
    }
    Reducer *rr = ff(&options);
    if (!rr) {
      Grouper_Free(grp);
      return NULL;
    }

    // Set the destination key for the grouper!
    RLookupKey *dstkey =
        RLookup_GetKey(&gstp->lookup, pr->alias, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
    Grouper_AddReducer(grp, rr, dstkey);
  }

  return Grouper_GetRP(grp);
}

/** Pushes a processor up the stack. Returns the newly pushed processor
 * @param req the request
 * @param rp the processor to push
 * @param rpUpstream previous processor (used as source for rp)
 * @return the processor passed in `rp`.
 */
static ResultProcessor *pushRP(AREQ *req, ResultProcessor *rp, ResultProcessor *rpUpstream) {
  rp->upstream = rpUpstream;
  rp->parent = &req->qiter;

  // In profile mode, we add an RPprofile before any RP to collect stats.
  if (IsProfile(req)) {
    rp = RPProfile_New(rp, &req->qiter);
  }

  req->qiter.endProc = rp;
  return rp;
}

static ResultProcessor *getGroupRP(AREQ *req, PLN_GroupStep *gstp, ResultProcessor *rpUpstream,
                                   QueryError *status) {
  AGGPlan *pln = &req->ap;
  RLookup *lookup = AGPLN_GetLookup(pln, &gstp->base, AGPLN_GETLOOKUP_PREV);
  ResultProcessor *groupRP = buildGroupRP(gstp, lookup, status);

  if (!groupRP) {
    return NULL;
  }

  // See if we need a LOADER group here...?
  RLookup *firstLk = AGPLN_GetLookup(pln, &gstp->base, AGPLN_GETLOOKUP_FIRST);

  if (firstLk == lookup) {
    // See if we need a loader step?
    const RLookupKey **kklist = NULL;
    for (RLookupKey *kk = firstLk->head; kk; kk = kk->next) {
      if ((kk->flags & RLOOKUP_F_DOCSRC) && (!(kk->flags & RLOOKUP_F_SVSRC))) {
        *array_ensure_tail(&kklist, const RLookupKey *) = kk;
      }
    }
    if (kklist != NULL) {
      ResultProcessor *rpLoader = RPLoader_New(firstLk, kklist, array_len(kklist));
      array_free(kklist);
      RS_LOG_ASSERT(rpLoader, "RPLoader_New failed");
      rpUpstream = pushRP(req, rpLoader, rpUpstream);
    }
  }

  return pushRP(req, groupRP, rpUpstream);
}

#define DEFAULT_LIMIT 10

static ResultProcessor *getArrangeRP(AREQ *req, AGGPlan *pln, const PLN_BaseStep *stp,
                                     QueryError *status, ResultProcessor *up) {
  ResultProcessor *rp = NULL;
  PLN_ArrangeStep astp_s = {.base = {.type = PLN_T_ARRANGE}};
  PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;

  if (!astp) {
    astp = &astp_s;
  }

  if (IsCount(req)) {
    rp = RPCounter_New();
    up = pushRP(req, rp, up);
    return up;
  }

  size_t limit = astp->offset + astp->limit;
  if (!limit) {
    limit = DEFAULT_LIMIT;
  }

  if (astp->sortKeys) {
    size_t nkeys = array_len(astp->sortKeys);
    astp->sortkeysLK = rm_malloc(sizeof(*astp->sortKeys) * nkeys);

    const RLookupKey **sortkeys = astp->sortkeysLK;

    RLookup *lk = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);

    for (size_t ii = 0; ii < nkeys; ++ii) {
      sortkeys[ii] = RLookup_GetKey(lk, astp->sortKeys[ii], RLOOKUP_F_NOINCREF);
      if (!sortkeys[ii]) {
        QueryError_SetErrorFmt(status, QUERY_ENOPROPKEY, "Property `%s` not loaded nor in schema",
                               astp->sortKeys[ii]);
        return NULL;
      }
    }

    rp = RPSorter_NewByFields(limit, sortkeys, nkeys, astp->sortAscMap);
    up = pushRP(req, rp, up);
  }

  // No sort? then it must be sort by score, which is the default.
  if (rp == NULL && (req->reqflags & QEXEC_F_IS_SEARCH)) {
    rp = RPSorter_NewByScore(limit);
    up = pushRP(req, rp, up);
  }

  if (astp->offset || (astp->limit && !rp)) {
    rp = RPPager_New(astp->offset, astp->limit);
    up = pushRP(req, rp, up);
  }

  return rp;
}

static ResultProcessor *getScorerRP(AREQ *req) {
  const char *scorer = req->searchopts.scorerName;
  if (!scorer) {
    scorer = DEFAULT_SCORER_NAME;
  }
  ScoringFunctionArgs scargs = {0};
  if (req->reqflags & QEXEC_F_SEND_SCOREEXPLAIN) {
    scargs.scrExp = rm_calloc(1, sizeof(RSScoreExplain));
  }
  ExtScoringFunctionCtx *fns = Extensions_GetScoringFunction(&scargs, scorer);
  RS_LOG_ASSERT(fns, "Extensions_GetScoringFunction failed");
  IndexSpec_GetStats(req->sctx->spec, &scargs.indexStats);
  scargs.qdata = req->ast.udata;
  scargs.qdatalen = req->ast.udatalen;
  ResultProcessor *rp = RPScorer_New(fns, &scargs);
  return rp;
}

static int hasQuerySortby(const AGGPlan *pln) {
  const PLN_BaseStep *bstp = AGPLN_FindStep(pln, NULL, NULL, PLN_T_GROUP);
  if (bstp != NULL) {
    const PLN_ArrangeStep *arng = (PLN_ArrangeStep *)AGPLN_FindStep(pln, NULL, bstp, PLN_T_ARRANGE);
    if (arng && arng->sortKeys) {
      return 1;
    }
  } else {
    // no group... just see if we have an arrange step
    const PLN_ArrangeStep *arng = (PLN_ArrangeStep *)AGPLN_FindStep(pln, NULL, NULL, PLN_T_ARRANGE);
    return arng && arng->sortKeys;
  }
  return 0;
}

#define PUSH_RP()                           \
  rpUpstream = pushRP(req, rp, rpUpstream); \
  rp = NULL;

/**
 * Builds the implicit pipeline for querying and scoring, and ensures that our
 * subsequent execution stages actually have data to operate on.
 */
static void buildImplicitPipeline(AREQ *req, QueryError *Status) {
  RedisSearchCtx *sctx = req->sctx;
  req->qiter.conc = &req->conc;
  req->qiter.sctx = sctx;
  req->qiter.err = Status;

  IndexSpecCache *cache = IndexSpec_GetSpecCache(req->sctx->spec);
  RS_LOG_ASSERT(cache, "IndexSpec_GetSpecCache failed")
  RLookup *first = AGPLN_GetLookup(&req->ap, NULL, AGPLN_GETLOOKUP_FIRST);

  RLookup_Init(first, cache);

  ResultProcessor *rp = RPIndexIterator_New(req->rootiter, req->timeoutTime);
  ResultProcessor *rpUpstream = NULL;
  req->qiter.rootProc = req->qiter.endProc = rp;
  PUSH_RP();

  /** Create a scorer if:
   *  * WITHSCORES is defined
   *  * there is no subsequent sorter within this grouping */
  if ((req->reqflags & QEXEC_F_SEND_SCORES) ||
      (!hasQuerySortby(&req->ap) && IsSearch(req) && !IsCount(req))) {
    rp = getScorerRP(req);
    PUSH_RP();
  }
}

/**
 * This handles the RETURN and SUMMARIZE keywords, which operate on the result
 * which is about to be returned. It is only used in FT.SEARCH mode
 */
int buildOutputPipeline(AREQ *req, QueryError *status) {
  AGGPlan *pln = &req->ap;
  ResultProcessor *rp = NULL, *rpUpstream = req->qiter.endProc;

  RLookup *lookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_LAST);
  // Add a LOAD step...
  const RLookupKey **loadkeys = NULL;
  if (req->outFields.explicitReturn) {
    // Go through all the fields and ensure that each one exists in the lookup stage
    for (size_t ii = 0; ii < req->outFields.numFields; ++ii) {
      const ReturnedField *rf = req->outFields.fields + ii;
      RLookupKey *lk = RLookup_GetKey(lookup, rf->name, RLOOKUP_F_NOINCREF | RLOOKUP_F_OCREAT);
      if (!lk) {
        // TODO: this is a dead code
        QueryError_SetErrorFmt(status, QUERY_ENOPROPKEY, "Property '%s' not loaded or in schema",
                               rf->name);
        goto error;
      }

      // change path to be used by loader
      lk->path = rf->path;

      *array_ensure_tail(&loadkeys, const RLookupKey *) = lk;
      // assign explicit output flag
      lk->flags |= RLOOKUP_F_EXPLICITRETURN;
    }
  }
  rp = RPLoader_New(lookup, loadkeys, loadkeys ? array_len(loadkeys) : 0);
  if (loadkeys) {
    array_free(loadkeys);
  }
  PUSH_RP();

  if (req->reqflags & QEXEC_F_SEND_HIGHLIGHT) {
    RLookup *lookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_LAST);
    for (size_t ii = 0; ii < req->outFields.numFields; ++ii) {
      ReturnedField *ff = req->outFields.fields + ii;
      RLookupKey *kk = RLookup_GetKey(lookup, ff->name, 0);
      if (!kk) {
        QueryError_SetErrorFmt(status, QUERY_ENOPROPKEY, "No such property `%s`", ff->name);
        goto error;
      } else if (!(kk->flags & (RLOOKUP_F_DOCSRC | RLOOKUP_F_SVSRC))) {
        // TODO: this is a dead code
        QueryError_SetErrorFmt(status, QUERY_EINVAL, "Property `%s` is not in document", ff->name);
        goto error;
      }
      ff->lookupKey = kk;
    }
    rp = RPHighlighter_New(&req->searchopts, &req->outFields, lookup);
    PUSH_RP();
  }

  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

int AREQ_BuildPipeline(AREQ *req, int options, QueryError *status) {
  if (!(options & AREQ_BUILDPIPELINE_NO_ROOT)) {
    buildImplicitPipeline(req, status);
  }

  AGGPlan *pln = &req->ap;
  ResultProcessor *rp = NULL, *rpUpstream = req->qiter.endProc;

  // Whether we've applied a SORTBY yet..
  int hasArrange = 0;

  for (const DLLIST_node *nn = pln->steps.next; nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);

    switch (stp->type) {
      case PLN_T_GROUP: {
        rpUpstream = getGroupRP(req, (PLN_GroupStep *)stp, rpUpstream, status);
        if (!rpUpstream) {
          goto error;
        }
        break;
      }

      case PLN_T_ARRANGE: {
        rp = getArrangeRP(req, pln, stp, status, rpUpstream);
        if (!rp) {
          goto error;
        }
        hasArrange = 1;
        rpUpstream = rp;
        break;
      }

      case PLN_T_APPLY:
      case PLN_T_FILTER: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)stp;
        // Ensure the lookups can actually find what they need
        RLookup *curLookup = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);
        mstp->parsedExpr = ExprAST_Parse(mstp->rawExpr, strlen(mstp->rawExpr), status);
        if (!mstp->parsedExpr) {
          goto error;
        }

        if (!ExprAST_GetLookupKeys(mstp->parsedExpr, curLookup, status)) {
          goto error;
        }

        if (stp->type == PLN_T_APPLY) {
          RLookupKey *dstkey =
              RLookup_GetKey(curLookup, stp->alias, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
          rp = RPEvaluator_NewProjector(mstp->parsedExpr, curLookup, dstkey);
        } else {
          rp = RPEvaluator_NewFilter(mstp->parsedExpr, curLookup);
        }
        PUSH_RP();
        break;
      }

      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)stp;
        RLookup *curLookup = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);
        RLookup *rootLookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_FIRST);
        if (curLookup != rootLookup) {
          QueryError_SetError(status, QUERY_EINVAL,
                              "LOAD cannot be applied after projectors or reducers");
          goto error;
        }
        // Get all the keys for this lookup...
        while (!AC_IsAtEnd(&lstp->args)) {
          const char *path = AC_GetStringNC(&lstp->args, NULL);
          if (*path == '@') {
            path++;
          }
          const char *name = path;

          RLookupKey *kk = RLookup_GetKey(curLookup, path, RLOOKUP_F_OEXCL | RLOOKUP_F_OCREAT);
          if (!kk) {
            // We only get a NULL return if the key already exists, which means
            // that we don't need to retrieve it again.
            continue;
          }

          if (AC_AdvanceIfMatch(&lstp->args, SPEC_AS_STR)) {
            int rv = AC_GetString(&lstp->args, &name, NULL, 0);
            if (rv != AC_OK) {
              QERR_MKBADARGS_FMT(status, "RETURN path AS name - must be accompanied with NAME");
              return REDISMODULE_ERR;
            } else if (!strncasecmp(name, SPEC_AS_STR, strlen(SPEC_AS_STR))) {
              QERR_MKBADARGS_FMT(status, "Alias for RETURN cannot be `AS`");
              return REDISMODULE_ERR;
            }
          }
          // set lookupkey name to name.
          // by defualt "name = path" 
          kk->name = name;
          lstp->keys[lstp->nkeys++] = kk;
        }
        if (lstp->nkeys) {
          rp = RPLoader_New(curLookup, lstp->keys, lstp->nkeys);
          PUSH_RP();
        }
        break;
      }
      case PLN_T_ROOT:
        // Placeholder step for initial lookup
        break;
      case PLN_T_DISTRIBUTE:
        // This is the root already
        break;
      case PLN_T_INVALID:
      case PLN_T__MAX:
        // not handled yet
        RS_LOG_ASSERT(0, "Oops");
    }
  }

  // If no LIMIT or SORT has been applied, do it somewhere here so we don't
  // return the entire matching result set!
  if (!hasArrange && (req->reqflags & QEXEC_F_IS_SEARCH)) {
    rp = getArrangeRP(req, pln, NULL, status, rpUpstream);
    if (!rp) {
      goto error;
    }
    rpUpstream = rp;
  }

  // If this is an FT.SEARCH command which requires returning of some of the
  // document fields, handle those options in this function
  if ((req->reqflags & QEXEC_F_IS_SEARCH) && !(req->reqflags & QEXEC_F_SEND_NOFIELDS)) {
    if (buildOutputPipeline(req, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

void AREQ_Free(AREQ *req) {
  // First, free the result processors
  ResultProcessor *rp = req->qiter.endProc;
  while (rp) {
    ResultProcessor *next = rp->upstream;
    rp->Free(rp);
    rp = next;
  }
  if (req->rootiter) {
    req->rootiter->Free(req->rootiter);
    req->rootiter = NULL;
  }

  // Go through each of the steps and free it..
  AGPLN_FreeSteps(&req->ap);

  QAST_Destroy(&req->ast);

  if (req->searchopts.stopwords) {
    StopWordList_Unref((StopWordList *)req->searchopts.stopwords);
  }

  ConcurrentSearchCtx_Free(&req->conc);

  // Finally, free the context. If we are a cursor, some more
  // cleanup is required since we also now own the
  // detached ("Thread Safe") context.
  RedisModuleCtx *thctx = NULL;
  if (req->sctx) {
    if (req->reqflags & QEXEC_F_IS_CURSOR) {
      thctx = req->sctx->redisCtx;
      req->sctx->redisCtx = NULL;
    }
    SearchCtx_Decref(req->sctx);
  }
  for (size_t ii = 0; ii < req->nargs; ++ii) {
    sdsfree(req->args[ii]);
  }
  if (req->searchopts.legacy.filters) {
    for (size_t ii = 0; ii < array_len(req->searchopts.legacy.filters); ++ii) {
      NumericFilter *nf = req->searchopts.legacy.filters[ii];
      if (nf) {
        NumericFilter_Free(req->searchopts.legacy.filters[ii]);
      }
    }
    array_free(req->searchopts.legacy.filters);
  }
  rm_free(req->searchopts.inids);
  FieldList_Free(&req->outFields);
  if (thctx) {
    RedisModule_FreeThreadSafeContext(thctx);
  }
  rm_free(req->args);
  rm_free(req);
}
