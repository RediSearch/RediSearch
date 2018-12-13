#include "aggregate.h"
#include "reducer.h"

#include <query.h>
#include <extension.h>
#include <result_processor.h>
#include <util/arr.h>
#include <rmutil/util.h>
#include "ext/default.h"
#include "extension.h"

/**
 * Ensures that the user has not requested one of the 'extended' features. Extended
 * in this case refers to reducers which re-create the search results.
 * @param areq the request
 * @param name the name of the option that requires simple mode. Used for error
 *   formatting
 * @param status the error object
 */
static int ensureSimpleMode(AREQ *areq, const char *name, QueryError *status) {
  if (areq->reqflags & QEXEC_F_IS_EXTENDED) {
    QueryError_SetErrorFmt(
        status, QUERY_EINVAL,
        "option `%s` is mutually exclusive with extended (i.e. aggregate) options", name);
    return 0;
  }
  areq->reqflags |= QEXEC_F_IS_SEARCH;
  return 1;
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
  free(field->highlightSettings.openTag);
  free(field->highlightSettings.closeTag);
  free(field->summarizeSettings.separator);
}

void FieldList_Free(FieldList *fields) {
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    ReturnedField_Free(fields->fields + ii);
  }
  ReturnedField_Free(&fields->defaultField);
  free(fields->fields);
}

ReturnedField *FieldList_GetCreateField(FieldList *fields, const char *name) {
  size_t foundIndex = -1;
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    if (!strcasecmp(fields->fields[ii].name, name)) {
      return fields->fields + ii;
    }
  }

  fields->fields = realloc(fields->fields, sizeof(*fields->fields) * ++fields->numFields);
  ReturnedField *ret = fields->fields + (fields->numFields - 1);
  memset(ret, 0, sizeof *ret);
  ret->name = strdup(name);
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

static int parseCursorSettings(AggregateRequest *req, ArgsCursor *ac, QueryError *status) {
  ACArgSpec specs[] = {{.name = "MAXIDLE",
                        .type = AC_ARGTYPE_UINT,
                        .target = &req->cursorChunkSize,
                        .intflags = AC_F_GE1},
                       {.name = "COUNT",
                        .type = AC_ARGTYPE_UINT,
                        .target = &req->cursorMaxIdle,
                        .intflags = AC_F_GE1},
                       {NULL}};

  int rv;
  ACArgSpec *errArg = NULL;
  if ((rv = AC_ParseArgSpec(ac, specs, &errArg)) != AC_OK) {
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

static int handleCommonArgs(AggregateRequest *req, ArgsCursor *ac, QueryError *status,
                            int allowLegacy) {
  int rv;
  // This handles the common arguments that are not stateful
  if (AC_AdvanceIfMatch(ac, "LIMIT")) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
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

    if (arng->limit == 0) {
      // LIMIT 0 0
      req->reqflags |= QEXEC_F_NOROWS;
    } else if (arng->limit > SEARCH_REQUEST_RESULTS_MAX) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "LIMIT exceeds maximum of %llu",
                             SEARCH_REQUEST_RESULTS_MAX);
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "SORTBY")) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
    if ((parseSortby(arng, ac, status, allowLegacy)) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "WITHSCHEMA")) {
    req->reqflags |= QEXEC_F_SEND_SCHEMA;
  } else if (AC_AdvanceIfMatch(ac, "ON_TIMEOUT")) {
    if (AC_NumRemaining(ac) < 1) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Need argument for ON_TIMEOUT");
      return ARG_ERROR;
    }
    const char *policystr = AC_GetStringNC(ac, NULL);
    req->tmoPolicy = TimeoutPolicy_Parse(policystr, strlen(policystr));
    if (req->tmoPolicy == TimeoutPolicy_Invalid) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "'%s' is not a valid timeout policy",
                             policystr);
      return ARG_ERROR;
    }
  } else if (AC_AdvanceIfMatch(ac, "WITHCURSOR")) {
    if (parseCursorSettings(req, ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else {
    return ARG_UNKNOWN;
  }

  return ARG_HANDLED;
}

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, int allowLegacy) {
  // Assume argument is at 'SORTBY'
  ArgsCursor subArgs = {0};
  int rv = AC_GetVarArgs(ac, &subArgs);
  int isLegacy = 0, legacyDesc = 0;

  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 0 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = SORTASCMAP_INIT;
  const char **keys = NULL;

  if (rv != AC_OK) {
    if (allowLegacy && AC_NumRemaining(ac) > 0) {
      // Mimic subArgs to contain the single field we already have
      isLegacy = 1;
      AC_GetSlice(ac, &subArgs, 1);
      if (AC_AdvanceIfMatch(ac, "DESC")) {
        legacyDesc = 1;
      } else if (AC_AdvanceIfMatch(ac, "ASC")) {
        legacyDesc = 0;
      }
    } else {
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
      if (array_len(keys) > SORTASCMAP_MAXFIELDS) {
        QERR_MKBADARGS_FMT(status, "Cannot sort by more than %lu fields", SORTASCMAP_MAXFIELDS);
        goto err;
      }

      const char *s = AC_GetStringNC(&subArgs, NULL);
      if (*s == '@') {
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
    options->legacy.gf = calloc(1, sizeof(*options->legacy.gf));
    if (GeoFilter_Parse(options->legacy.gf, ac, status) != REDISMODULE_OK) {
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
  ArgsCursor returnFields = {0};
  ArgsCursor inKeys = {0};
  ArgsCursor inFields = {0};
  ACArgSpec querySpecs[] = {
      {.name = "INFIELDS", .type = AC_ARGTYPE_SUBARGS, .target = &inFields},  // Comment
      {.name = "SLOP",
       .type = AC_ARGTYPE_INT,
       .target = &searchOpts->slop,
       .intflags = AC_F_COALESCE},
      {.name = "LANGUAGE", .type = AC_ARGTYPE_STRING, .target = &searchOpts->language},
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
      if (!ensureSimpleMode(req, "SUMMARIZE", status)) {
        return REDISMODULE_ERR;
      }
      if (ParseSummarize(ac, &req->outFields) == REDISMODULE_ERR) {
        QERR_MKBADARGS_FMT(status, "Bad arguments for SUMMARIZE");
        return REDISMODULE_ERR;
      }
      req->reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if (AC_AdvanceIfMatch(ac, "HIGHLIGHT")) {
      if (!ensureSimpleMode(req, "HIGHLIGHT", status)) {
        return REDISMODULE_ERR;
      }
      if (ParseHighlight(ac, &req->outFields) == REDISMODULE_ERR) {
        QERR_MKBADARGS_FMT(status, "Bad arguments for HIGHLIGHT");
        return REDISMODULE_ERR;
      }
      req->reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if ((rv = parseQueryLegacyArgs(ac, searchOpts, status) != ARG_UNKNOWN)) {
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

  searchOpts->inkeys = (const char **)inKeys.objs;
  searchOpts->ninkeys = inKeys.argc;
  searchOpts->legacy.infields = (const char **)inFields.objs;
  searchOpts->legacy.ninfields = inFields.argc;

  if (AC_IsInitialized(&returnFields)) {
    if (!ensureSimpleMode(req, "RETURN", status)) {
      return REDISMODULE_ERR;
    }

    req->outFields.explicitReturn = 1;
    if (returnFields.argc == 0) {
      req->reqflags |= QEXEC_F_SEND_NOFIELDS;
    }

    while (!AC_IsAtEnd(&returnFields)) {
      const char *name = AC_GetStringNC(&returnFields, NULL);
      ReturnedField *f = FieldList_GetCreateField(&req->outFields, name);
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
  char *dup = strndup(out, sdslen(out));
  sdsfree(out);
  return dup;
}

static void groupStepFree(PLN_BaseStep *base) {
  PLN_GroupStep *g = (PLN_GroupStep *)base;
  if (g->reducers) {
    size_t nreducers = array_len(g->reducers);
    for (size_t ii = 0; ii < nreducers; ++ii) {
      PLN_Reducer *gr = g->reducers + ii;
      free(gr->alias);
    }
    array_free(g->reducers);
  }

  RLookup_Cleanup(&g->lookup);
  free(base);
}

static int buildReducer(PLN_GroupStep *g, PLN_Reducer *gr, ArgsCursor *ac, const char *name,
                        QueryError *status) {
  // Just a list of functions..
  gr->name = name;
  int rv = AC_GetVarArgs(ac, &gr->args);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, name, rv);
    return REDISMODULE_ERR;
  }

  const char *alias = NULL;
  // See if there is an alias
  if (AC_AdvanceIfMatch(ac, "AS")) {
    rv = AC_GetString(ac, &alias, NULL, 0);
    if (rv != AC_OK) {
      QERR_MKBADARGS_AC(status, "AS", rv);
      return REDISMODULE_ERR;
    }
  }
  if (alias == NULL) {
    gr->alias = getReducerAlias(g, name, &gr->args);
  } else {
    gr->alias = strdup(alias);
  }
  return REDISMODULE_OK;
}

static void genericStepFree(PLN_BaseStep *p) {
  free(p);
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

  PLN_GroupStep *gstp = calloc(1, sizeof(*gstp));
  gstp->properties = (const char **)groupArgs.objs;
  gstp->nproperties = groupArgs.argc;
  gstp->base.dtor = groupStepFree;
  gstp->base.type = PLN_T_GROUP;
  AGPLN_AddStep(&req->ap, &gstp->base);

  while (AC_AdvanceIfMatch(ac, "REDUCE")) {
    PLN_Reducer *cur;
    const char *name;
    if (AC_GetString(ac, &name, NULL, 0) != AC_OK) {
      QERR_MKBADARGS_AC(status, "REDUCE", rv);
      return REDISMODULE_ERR;
    }
    cur = array_ensure_tail(&gstp->reducers, PLN_Reducer);

    if (buildReducer(gstp, cur, ac, name, status) != REDISMODULE_OK) {
      printf("Error for reducer!\n");
      goto error;
    }
  }
  gstp->idx = req->serial++;
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static int handleApplyOrFilter(AREQ *req, ArgsCursor *ac, QueryError *status, int isApply) {
  // Parse filters!
  const char *expr = NULL;
  int rv = AC_GetString(ac, &expr, NULL, 0);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "APPLY/FILTER", rv);
    return REDISMODULE_ERR;
  }

  PLN_MapFilterStep *stp = calloc(1, sizeof(*stp));
  stp->base.dtor = genericStepFree;
  stp->base.type = isApply ? PLN_T_APPLY : PLN_T_FILTER;
  stp->rawExpr = expr;
  AGPLN_AddStep(&req->ap, &stp->base);

  if (isApply) {
    if (AC_AdvanceIfMatch(ac, "AS")) {
      const char *alias;
      if (AC_GetString(ac, &alias, NULL, 0) != AC_OK) {
        QERR_MKBADARGS_FMT(status, "AS needs argument");
        goto error;
      }
      stp->base.alias = strdup(alias);
    } else {
      stp->base.alias = strdup(expr);
    }
  }
  return REDISMODULE_OK;

error:
  if (stp) {
    stp->base.dtor(&stp->base);
  }
  return REDISMODULE_ERR;
}

static int handleLoad(AREQ *req, ArgsCursor *ac, QueryError *status) {
  ArgsCursor loadfields = {0};
  int rc = AC_GetVarArgs(ac, &loadfields);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(status, "LOAD", rc);
    return REDISMODULE_ERR;
  }

  PLN_LoadStep *lstp = calloc(1, sizeof(*lstp));
  lstp->base.type = PLN_T_LOAD;
  lstp->args = loadfields;
  lstp->keys = calloc(loadfields.argc, sizeof(*lstp->keys));

  AGPLN_AddStep(&req->ap, &lstp->base);
  return REDISMODULE_OK;
}

AREQ *AREQ_New(void) {
  return calloc(1, sizeof(AREQ));
}

int AREQ_Compile(AREQ *req, RedisModuleString **argv, int argc, QueryError *status) {
  req->args = malloc(sizeof(*req->args) * argc);
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
  // TODO: Default should be all fields
  searchOpts->fieldmask = RS_FIELDMASK_ALL;
  searchOpts->slop = -1;

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
      rv = handleCommonArgs(req, &ac, status, 0);
      if (rv == ARG_ERROR) {
        goto error;
      } else if (rv == ARG_UNKNOWN) {
        QueryError_FmtUnknownArg(status, &ac, "<main>");
        goto error;
      }
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
  }
  if (opts->legacy.gf) {
    QAST_GlobalFilterOptions legacyOpts = {.geo = opts->legacy.gf};
    QAST_SetGlobalFilters(ast, &legacyOpts);
  }

  if (opts->inkeys) {
    opts->inids = malloc(sizeof(*opts->inids) * opts->ninkeys);
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

/* A callback called when we regain concurrent execution context, and the index spec key is
 * reopened. We protect against the case that the spec has been deleted during query execution
 */
static void onReopen(RedisModuleKey *k, void *privdata) {
  abort();
  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);
  AREQ *req = privdata;

  // If we don't have a spec or key - we abort the query
  if (k == NULL || sp == NULL) {
    req->qiter.state = QITR_S_ABORTED;
    req->sctx->spec = NULL;
    return;
  }

  // The spec might have changed while we were sleeping - for example a realloc of the doc table
  req->sctx->spec = sp;

  // FIXME: Per-query!!
  if (req->tmoMS > 0) {
    // Check the elapsed processing time
    static struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);

    long long durationNS = (long long)1000000000 * (now.tv_sec - req->qiter.startTime.tv_sec) +
                           (now.tv_nsec - req->qiter.startTime.tv_nsec);
    // printf("Elapsed: %zdms\n", durationNS / 1000000);
    // Abort on timeout
    if (durationNS > req->tmoMS * 1000000) {
      if (req->reqflags & QEXEC_F_IS_CURSOR) {
        req->pause = 1;
      } else {
        req->qiter.state = QITR_S_TIMEDOUT;
      }
    }
  }
}

int AREQ_ApplyContext(AREQ *req, RedisSearchCtx *sctx, QueryError *status) {
  // Sort through the applicable options:
  req->sctx = sctx;
  IndexSpec *index = sctx->spec;
  RSSearchOptions *opts = &req->searchopts;

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

  if (opts->language && !IsSupportedLanguage(opts->language, strlen(opts->language))) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL, "No such language %s", opts->language);
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
    QAST_Expand(ast, opts->expanderName, opts, sctx);
  }

  /** Handle concurrent context */
  if (!(req->reqflags & QEXEC_F_SAFEMODE)) {
    ConcurrentSearchCtx *conc = &req->conc;
    ConcurrentSearch_AddKey(conc, sctx->key, REDISMODULE_READ, sctx->keyName, onReopen, req, NULL,
                            ConcurrentKey_SharedKeyString);
    sctx->conc = conc;
  }

  req->rootiter = QAST_Iterate(ast, opts, sctx, status);
  if (!req->rootiter) {
    return REDISMODULE_ERR;
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
      QueryError_SetErrorFmt(err, QUERY_ENOREDUCER, "No such reducer: %s", pr->name);
      return NULL;
    }
    Reducer *rr = ff(&options);
    if (!rr) {
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
      assert(rpLoader);
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

  size_t limit = astp->offset + astp->limit;
  if (!limit) {
    limit = DEFAULT_LIMIT;
  }

  if (astp->sortKeys) {
    size_t nkeys = array_len(astp->sortKeys);
    astp->sortkeysLK = malloc(sizeof(*astp->sortKeys) * nkeys);

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
  ExtScoringFunctionCtx *fns = Extensions_GetScoringFunction(&scargs, scorer);
  if (!fns) {
    fns = Extensions_GetScoringFunction(&scargs, DEFAULT_SCORER_NAME);
  }
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

int AREQ_BuildPipeline(AREQ *req, QueryError *status) {
  AGGPlan *pln = &req->ap;
  RedisSearchCtx *sctx = req->sctx;
  req->qiter.conc = sctx->conc;
  req->qiter.sctx = sctx;

  IndexSpecCache *cache = IndexSpec_GetSpecCache(req->sctx->spec);
  assert(cache);
  RLookup *first = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_FIRST);

  RLookup_Init(first, cache);

  // Whether we've applied a SORTBY yet..
  int hasArrange = 0;

#define PUSH_RP()                           \
  rpUpstream = pushRP(req, rp, rpUpstream); \
  rp = NULL;

  ResultProcessor *rp = RPIndexIterator_New(req->rootiter);
  ResultProcessor *rpUpstream = NULL;
  req->qiter.rootProc = req->qiter.endProc = rp;
  PUSH_RP();

  /** Create a scorer if there is no subsequent sorter within this grouping */
  if (!hasQuerySortby(pln)) {
    rp = getScorerRP(req);
    PUSH_RP();
  }

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
          const char *s = AC_GetStringNC(&lstp->args, NULL);
          if (*s == '@') {
            s++;
          }
          const RLookupKey *kk = RLookup_GetKey(curLookup, s, RLOOKUP_F_OEXCL | RLOOKUP_F_OCREAT);
          if (!kk) {
            // printf("Ignoring already-loaded key %s\n", s);
            continue;
          }
          lstp->keys[lstp->nkeys++] = kk;
        }
        rp = RPLoader_New(curLookup, lstp->keys, lstp->nkeys);
        PUSH_RP();
        break;
      }
      case PLN_T_ROOT:
        break;
      case PLN_T_DISTRIBUTE:
      case PLN_T_INVALID:
      case PLN_T__MAX:
        // not handled yet
        abort();
    }
  }

  if (!hasArrange && (req->reqflags & QEXEC_F_IS_SEARCH)) {
    rp = getArrangeRP(req, pln, NULL, status, rpUpstream);
    if (!rp) {
      goto error;
    }
    rpUpstream = rp;
  }

  if ((req->reqflags & QEXEC_F_IS_SEARCH) && !(req->reqflags & QEXEC_F_SEND_NOFIELDS)) {
    RLookup *lookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_LAST);
    // Add a LOAD step...
    const RLookupKey **loadkeys = NULL;
    size_t nloadkeys = 0;
    if (req->outFields.explicitReturn) {
      // Go through all the fields and ensure that each one exists in the lookup stage
      for (size_t ii = 0; ii < req->outFields.numFields; ++ii) {
        const ReturnedField *rf = req->outFields.fields + ii;
        RLookupKey *lk = RLookup_GetKey(lookup, rf->name, RLOOKUP_F_NOINCREF);
        if (!lk) {
          QueryError_SetErrorFmt(status, QUERY_ENOPROPKEY, "Property '%s' not loaded or in schema",
                                 rf->name);
          goto error;
        }
        *array_ensure_tail(&loadkeys, const RLookupKey *) = lk;
        nloadkeys++;
      }
    }
    rp = RPLoader_New(lookup, loadkeys, nloadkeys);
    if (loadkeys) {
      array_free(loadkeys);
    }
    PUSH_RP();

    if (req->reqflags & QEXEC_F_SEND_HIGHLIGHT) {
      rp = RPHighlighter_New(&req->searchopts, &req->outFields, lookup);
      PUSH_RP();
    }
  }

  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

/**
 * Get the sorting key of the result. This will be the sorting key of the last
 * RLookup registry. Returns NULL if there is no sorting key
 */
static const RSValue *getSortKey(AREQ *req, const SearchResult *r) {
  PLN_ArrangeStep *astp = AGPLN_GetArrangeStep(&req->ap);
  if (!astp) {
    return NULL;
  }
  return RLookup_GetItem(astp->sortkeysLK[0], &r->rowdata);
}

static size_t serializeResult(AREQ *req, RedisModuleCtx *outctx, const SearchResult *r) {
  const uint32_t options = req->reqflags;
  const RSDocumentMetadata *dmd = r->dmd;
  size_t count = 0;

  if (dmd && req->reqflags & QEXEC_F_IS_SEARCH) {
    size_t n;
    const char *s = DMD_KeyPtrLen(dmd, &n);
    RedisModule_ReplyWithStringBuffer(outctx, s, n);
    count++;
  }

  if (options & QEXEC_F_SEND_SCORES) {
    RedisModule_ReplyWithDouble(outctx, r->score);
    count++;
  }
  if (options & QEXEC_F_SEND_PAYLOADS) {
    count++;
    if (dmd && dmd->payload) {
      RedisModule_ReplyWithStringBuffer(outctx, dmd->payload->data, dmd->payload->len);
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }

  if ((options & QEXEC_F_SEND_SORTKEYS)) {
    count++;
    const RSValue *sortkey = getSortKey(req, r);
    if (sortkey) {
      switch (sortkey->t) {
        case RSValue_Number:
          /* Serialize double - by prepending "%" to the number, so the coordinator/client can
           * tell it's a double and not just a numeric string value */
          RedisModule_ReplyWithString(
              outctx, RedisModule_CreateStringPrintf(outctx, "#%.17g", sortkey->numval));
          break;
        case RSValue_String:
          /* Serialize string - by prepending "$" to it */

          RedisModule_ReplyWithString(
              outctx, RedisModule_CreateStringPrintf(outctx, "$%s", sortkey->strval));
          break;
        case RSValue_RedisString:
          RedisModule_ReplyWithString(
              outctx, RedisModule_CreateStringPrintf(
                          outctx, "$%s", RedisModule_StringPtrLen(sortkey->rstrval, NULL)));
          break;
        default:
          // NIL, or any other type:
          RedisModule_ReplyWithNull(outctx);
      }
    } else {
      RedisModule_ReplyWithNull(outctx);
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    count++;
    size_t nfields = 0;
    REDISMODULE_BEGIN_ARRAY(outctx);
    RLookup *lk = AGPLN_GetLookup(&req->ap, NULL, AGPLN_GETLOOKUP_LAST);

    for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
      if (kk->flags & RLOOKUP_F_HIDDEN) {
        // printf("Skipping hidden field %s/%p\n", kk->name, kk);
        continue;
      }
      nfields++;
      RedisModule_ReplyWithSimpleString(outctx, kk->name);
      const RSValue *v = RLookup_GetItem(kk, &r->rowdata);
      if (!v) {
        RedisModule_ReplyWithNull(outctx);
      } else {
        RSValue_SendReply(outctx, v);
      }
    }
    REDISMODULE_END_ARRAY(outctx, nfields * 2);
  }
  return count;
}

void AREQ_Execute(AREQ *req, RedisModuleCtx *outctx) {
  SearchResult r = {0};
  size_t nelem = 0, nrows = 0;
  size_t limit = -1;
  int rc;

  // char *dumped = Query_DumpExplain(&req->ast, req->sctx->spec);
  // printf("dumped: %s\n", dumped);
  // free(dumped);

  const int isCursor = req->reqflags & QEXEC_F_IS_CURSOR;
  const int firstCursor = !(req->reqflags & QEXEC_S_SENTONE);

  if (isCursor) {
    if ((limit = req->cursorChunkSize) == 0) {
      limit = -1;
    }
  }

  ResultProcessor *rp = req->qiter.endProc;

  RedisModule_ReplyWithArray(outctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  rc = rp->Next(rp, &r);
  RedisModule_ReplyWithLongLong(outctx, req->qiter.totalResults);
  nelem++;

  if (rc == RS_RESULT_OK) {
    if (++nrows < limit && !(req->reqflags & QEXEC_F_NOROWS)) {
      nelem += serializeResult(req, outctx, &r);
    }
    SearchResult_Clear(&r);
  }

  if (rc != RS_RESULT_OK || (req->reqflags & QEXEC_F_NOROWS)) {
    goto rows_done;
  }

  while (++nrows < limit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
    // Serialize it as a search result
    nelem += serializeResult(req, outctx, &r);
    SearchResult_Clear(&r);
  }

rows_done:
  req->stateflags |= QEXEC_S_SENTONE;
  // TODO: Check for errors in `rc`

  SearchResult_Destroy(&r);
  RedisModule_ReplySetArrayLength(outctx, nelem);
  AREQ_Free(req);
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

  // Finally, free the context
  if (req->sctx) {
    SearchCtx_Decref(req->sctx);
  }
  free(req);
}