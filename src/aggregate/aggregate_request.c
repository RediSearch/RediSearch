#include "aggregate.h"
#include "reducer.h"

#include <query.h>
#include <extension.h>
#include <result_processor.h>
#include <util/arr.h>
#include <rmutil/util.h>

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

static int parseSortbyArgs(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status,
                           int allowLegacy);

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
  } else if (AC_AdvanceIfMatch(ac, "SORTBY")) {
    PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
    if ((parseSortbyArgs(arng, ac, status, allowLegacy)) != REDISMODULE_OK) {
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

static int parseSortbyArgs(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status,
                           int allowLegacy) {
  // Assume argument is at 'SORTBY'
  ArgsCursor subArgs = {0};
  int rv = AC_GetVarArgs(ac, &subArgs);
  int isLegacy = 0, legacyDesc = 0;

  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 2 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = 0xFFFFFFFFFFFFFFFF;
  size_t n = 0;
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
      ascMap &= ~(1 << (n - 1));
    }
  } else {
    // since ASC/DESC are optional, we need a stateful parser
    // state 0 means we are open only to property names
    // state 1 means we are open to either a new property or ASC/DESC
    int state = 0;
    while (!AC_IsAtEnd(&subArgs) && n < sizeof(ascMap) * 8) {
      // New properties are accepted in either state
      const char *s = AC_GetStringNC(&subArgs, NULL);
      if (*s == '@') {
        s++;
        keys = array_append(keys, s);
      } else if (state == 0) {
        goto err;
      } else if (!strcasecmp(s, "asc")) {
        // For as - we put a 1 in the map. We don't actually need to, this is just for readability
        ascMap |= 1 << (n - 1);
        // switch back to state 0, ASC/DESC cannot follow ASC
        state = 0;
      } else if (!strcasecmp(s, "desc")) {
        // We turn the current bit to 0 meaning desc for the Nth property
        ascMap &= ~(1 << (n - 1));
        // switch back to state 0, ASC/DESC cannot follow ASC
        state = 0;
      } else {
        // Unknown token - neither a property nor ASC/DESC
        goto err;
      }
    }
  }

  // Parse optional MAX

  if (AC_AdvanceIfMatch(&subArgs, "MAX")) {
    unsigned mx = 0;
    if ((rv = AC_GetUnsigned(&subArgs, &mx, 0) != AC_OK)) {
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
    NumericFilter *cur = array_ensure_tail(&options->legacy.filters, NumericFilter);
    if (AC_NumRemaining(ac) < 3) {
      QERR_MKBADARGS_FMT(status, "FILTER requires 3 arguments");
    }
    cur->fieldName = AC_GetStringNC(ac, NULL);
    if (NumericFilter_Parse(cur, ac, status) != REDISMODULE_OK) {
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

static int parseQueryArgs(ArgsCursor *ac, AggregateRequest *req, RSSearchOptions *searchOpts,
                          AggregatePlan *plan, QueryError *status) {
  // Parse query-specific arguments..
  ArgsCursor returnFields = {0};
  ArgsCursor inKeys = {0};
  ACArgSpec querySpecs[] = {
      {.name = "INFIELDS", .type = AC_ARGTYPE_SUBARGS, .target = searchOpts},  // Comment
      {.name = "SLOP",
       .type = AC_ARGTYPE_INT,
       .target = &searchOpts->slop,
       .intflags = AC_F_COALESCE},
      {.name = "LANGUAGE", .type = AC_ARGTYPE_STRING, .target = &searchOpts->language},
      {.name = "EXPANDER", .type = AC_ARGTYPE_STRING, .target = &searchOpts->expanderName},
      {.name = "INKEYS", .type = AC_ARGTYPE_SUBARGS, .target = &inKeys},
      {.name = "SCORER", .type = AC_ARGTYPE_STRING, .target = &searchOpts->scorerName},
      {.name = "RETURN", .type = AC_ARGTYPE_SUBARGS, .target = &returnFields},
      {AC_MKBITFLAG("VERBATIM", &searchOpts->flags, Search_Verbatim)},
      {AC_MKBITFLAG("WITHSCORES", &req->reqflags, QEXEC_F_SEND_SCORES)},
      {AC_MKBITFLAG("WITHSORTKEYS", &req->reqflags, QEXEC_F_SEND_SORTKEYS)},
      {AC_MKBITFLAG("WITHPAYLOADS", &req->reqflags, QEXEC_F_SEND_PAYLOADS)},
      {AC_MKBITFLAG("NOCONTENT", &req->reqflags, QEXEC_F_SEND_NOFIELDS)},
      {AC_MKBITFLAG("NOSTOPWORDS", &searchOpts->flags, Search_NoStopwrods)},
      {.name = "PAYLOAD",
       .type = AC_ARGTYPE_STRING,
       .target = &searchOpts->payload,
       .len = &searchOpts->npayload},
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
      printf("RETURN %s\n", name);
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
  // FIXME!
  PLN_GroupStep *g = (PLN_GroupStep *)base;
  PLN_Reducer *gr = g->reducers;
  // the reducer func itself is const char and should not be freed
  free(gr->alias);
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

static PLN_BaseStep *parseGroupbyArgs(ArgsCursor *ac, AggregateRequest *req, QueryError *status) {
  ArgsCursor groupArgs = {0};
  const char *s;
  AC_GetString(ac, &s, NULL, AC_F_NOADVANCE);
  int rv = AC_GetVarArgs(ac, &groupArgs);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "GROUPBY", rv);
    return NULL;
  }

  // Number of fields.. now let's see the reducers

  PLN_GroupStep *ret = calloc(1, sizeof(*ret));
  ret->properties = (const char **)groupArgs.objs;
  ret->nproperties = groupArgs.argc;
  ret->base.dtor = groupStepFree;
  ret->base.type = PLN_T_GROUP;

  while (AC_AdvanceIfMatch(ac, "REDUCE")) {
    PLN_Reducer *cur;
    const char *name;
    if (AC_GetString(ac, &name, NULL, 0) != AC_OK) {
      QERR_MKBADARGS_AC(status, "REDUCE", rv);
      return NULL;
    }
    cur = array_ensure_tail(&ret->reducers, PLN_Reducer);

    // Get the name
    int rv = buildReducer(ret, cur, ac, name, status);
  }
  ret->idx = req->serial++;
  return &ret->base;
}

static int handleApplyOrFilter(AggregateRequest *req, ArgsCursor *ac, QueryError *status,
                               int isApply) {
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

  if (isApply && AC_AdvanceIfMatch(ac, "AS")) {
    const char *alias = NULL;
    if (AC_GetString(ac, &alias, NULL, 0) != AC_OK) {
      QERR_MKBADARGS_FMT(status, "AS needs argument");
      goto error;
    }
  }
  return REDISMODULE_OK;

error:
  if (stp) {
    stp->base.dtor(&stp->base);
  }
  return REDISMODULE_ERR;
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
  if (parseQueryArgs(&ac, req, searchOpts, &req->ap, status) != REDISMODULE_OK) {
    goto error;
  }

  int hasLoad = 0;

  // Now we have a 'compiled' plan. Let's get some more options..

  while (!AC_IsAtEnd(&ac)) {
    int rv = handleCommonArgs(req, &ac, status, 0);
    if (rv == ARG_HANDLED) {
      continue;
    } else if (rv == ARG_ERROR) {
      goto error;
    }

    if (AC_AdvanceIfMatch(&ac, "GROUPBY")) {
      if (!ensureExtendedMode(req, "GROUPBY", status)) {
        goto error;
      }
      PLN_BaseStep *groupStep = parseGroupbyArgs(&ac, req, status);
      if (groupStep) {
        AGPLN_AddStep(&req->ap, groupStep);
      } else {
        goto error;
      }
    } else if (AC_AdvanceIfMatch(&ac, "APPLY")) {
      int rv = handleApplyOrFilter(req, &ac, status, 1);
      if (rv != REDISMODULE_OK) {
        goto error;
      }
    } else if (AC_AdvanceIfMatch(&ac, "LOAD")) {
      /* IGNORE LOAD!! */

      // if (hasLoad) {
      //   QueryError_SetError(status, QUERY_EPARSEARGS, "LOAD fields already specified!");
      //   goto error;
      // }
      // hasLoad = 1;
      // ArgsCursor loadFields = {0};
      // if ((rv = AC_GetVarArgs(&ac, &loadFields) != AC_OK)) {
      //   QERR_MKBADARGS_AC(status, "LOAD", rv);
      //   goto error;
      // }
      // AggregateStep *loadStep = PLN_AllocStep(AggregateStep_Load);
      // loadStep->load.fl = (FieldList){0};
      // loadStep->load.keys = RS_NewMultiKeyFromAC(&loadFields, 1, 1);
    } else if (AC_AdvanceIfMatch(&ac, "FILTER")) {
      int rv = handleApplyOrFilter(req, &ac, status, 0);
      if (rv != REDISMODULE_OK) {
        goto error;
      }
    } else {
      rv = handleCommonArgs(req, &ac, status, 0);
      if (rv == ARG_ERROR) {
        goto error;
      } else if (rv == ARG_UNKNOWN) {
        const char *s = AC_GetStringNC(&ac, NULL);
        QERR_MKBADARGS_FMT(status, "Unknown argument: %s", s);
        goto error;
      }
    }
  }
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static void applyGlobalFilters(RSSearchOptions *opts, QueryAST *ast, const RedisSearchCtx *sctx) {
  /**
   * The following blocks will set filter options on the entire query
   */
  if (opts->legacy.filters) {
    for (size_t ii = 0; ii < array_len(opts->legacy.filters); ++ii) {
      QAST_GlobalFilterOptions legacyFilterOpts = {.numeric = opts->legacy.filters + ii};
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
  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);
  AggregateRequest *req = privdata;

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
  // q->docTable = &sp->docs;
}

int AREQ_ApplyContext(AREQ *req, RedisSearchCtx *sctx, QueryError *status) {
  // Sort through the applicable options:
  req->sctx = sctx;
  IndexSpec *index = sctx->spec;
  RSSearchOptions *opts = &req->searchopts;
  // Go through the query options and see what else needs to be filled in!
  // 1) INFIELDS
  for (size_t ii = 0; ii < opts->legacy.ninfields; ++ii) {
    const char *s = opts->legacy.infields[ii];
    t_fieldMask bit = IndexSpec_GetFieldBit(index, s, strlen(s));
    opts->fieldmask |= bit;
  }

  if (opts->language && !IsSupportedLanguage(opts->language, strlen(opts->language))) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL, "No such language %s", opts->language);
    return REDISMODULE_ERR;
  }
  if (opts->scorerName && Extensions_GetScoringFunction(NULL, opts->scorerName) == NULL) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL, "No such scorer %s", opts->scorerName);
    return REDISMODULE_ERR;
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
    ReducerOptions options = {.name = pr->name, .args = &pr->args, .status = err};
    ReducerFactory ff = RDCR_GetFactory(pr->name);
    if (!ff) {
      // No such reducer!
      QueryError_SetErrorFmt(err, QUERY_ENOREDUCER, "No such reducer: %s", pr->name);
      return NULL;
    }
    Reducer *rr = ff(&options);
    if (!ff(&options)) {
      return NULL;
    }

    // Set the destination key for the grouper!
    rr->dstkey = RLookup_GetKey(&gstp->lookup, pr->alias, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
    Grouper_AddReducer(grp, rr);
  }

  return Grouper_GetRP(grp);

error:
  return NULL;
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
  printf("FirstLK: %p. CurLookup: %p\n", firstLk, lookup);

  if (firstLk == lookup) {
    // See if we need a loader step?
    const RLookupKey **kklist = NULL;
    for (RLookupKey *kk = firstLk->head; kk; kk = kk->next) {
      printf("Checking RLookupKey: %s; flags=%d\n", kk->name, kk->flags);
      if ((kk->flags & RLOOKUP_F_DOCSRC) && (!(kk->flags & RLOOKUP_F_SVSRC))) {
        *array_ensure_tail(&kklist, const RLookupKey *) = kk;
      }
    }
    if (kklist != NULL) {
      printf("Creating initial loader...\n");
      ResultProcessor *rpLoader = RPLoader_New(firstLk, kklist, array_len(kklist));
      array_free(kklist);
      assert(rpLoader);
      rpUpstream = pushRP(req, rpLoader, rpUpstream);
    }
  }

  return pushRP(req, groupRP, rpUpstream);
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

#define PUSH_RP()                           \
  rpUpstream = pushRP(req, rp, rpUpstream); \
  rp = NULL;

  ResultProcessor *rp = RPIndexIterator_New(req->rootiter);
  ResultProcessor *rpUpstream = rp;
  req->qiter.rootProc = req->qiter.endProc = rp;
  PUSH_RP();

  // Add scorer
  RSIndexStats stats = {0};
  IndexSpec_GetStats(sctx->spec, &stats);

  rp = RPScorer_New(&req->searchopts, &stats);
  PUSH_RP();
  for (const DLLIST_node *nn = pln->steps.prev; nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    printf("Processing STP::Type: %d\n", stp->type);

    switch (stp->type) {
      case PLN_T_GROUP: {
        rpUpstream = getGroupRP(req, (PLN_GroupStep *)stp, rpUpstream, status);
        if (!rpUpstream) {
          goto error;
        }
        break;
      }

      case PLN_T_ARRANGE: {
        PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;
        if (astp->sortKeys) {
          size_t nkeys = array_len(astp->sortKeys);
          const RLookupKey *sortkeys[nkeys];
          RLookup *lk = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);

          for (size_t ii = 0; ii < nkeys; ++ii) {
            sortkeys[ii] = RLookup_GetKey(lk, astp->sortKeys[ii], RLOOKUP_F_NOINCREF);
            if (!sortkeys[ii]) {
              QueryError_SetErrorFmt(status, QUERY_ENOPROPKEY,
                                     "Property `%s` not loaded nor in schema", astp->sortKeys[ii]);
              goto error;
            }
          }
          rp = RPSorter_NewByFields(astp->offset + astp->limit, sortkeys, nkeys, astp->sortAscMap);
          PUSH_RP();
        }
        if (astp->offset) {
          rp = RPPager_New(astp->offset, astp->limit);
          PUSH_RP();
        }
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
    }
  }

  if (!(req->reqflags & QEXEC_F_IS_EXTENDED) && !(req->reqflags & QEXEC_F_SEND_NOFIELDS)) {
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
static const RSValue *getSortKey(AggregateRequest *req, const SearchResult *r) {
  const RLookup *last = AGPLN_GetLookup(&req->ap, NULL, AGPLN_GETLOOKUP_LAST);

  // Scan through the keys
  const RLookupKey *k = RLookup_FindKeyWith(last, RLOOKUP_F_SORTKEY);
  if (!k) {
    return NULL;
  }

  // Get the value
  return RLookup_GetItem(k, &r->rowdata);
}

static size_t serializeResult(AggregateRequest *req, RedisModuleCtx *outctx,
                              const SearchResult *r) {
  const uint32_t options = req->reqflags;
  const RSDocumentMetadata *dmd = r->dmd;
  size_t count = 0;

  if (dmd) {
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
          /* Serialize double - by prepending "%" to the number, so the coordinator/client can tell
           * it's a double and not just a numeric string value */
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

void AREQ_Execute(AggregateRequest *req, RedisModuleCtx *outctx) {
  SearchResult r = {0};
  size_t nelem = 0, nrows = 0;
  size_t limit = -1;
  int rc;

  const int isCursor = req->reqflags & QEXEC_F_IS_CURSOR;
  const int firstCursor = !(req->reqflags & QEXEC_S_SENTONE);

  if (isCursor) {
    if ((limit = req->cursorChunkSize) == 0) {
      limit = -1;
    }
  }

  ResultProcessor *rp = req->qiter.endProc;
  printf("TopLevel RP Name: %s. Limit=%lu\n", rp->name, limit);

  RedisModule_ReplyWithArray(outctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while ((rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
    // Serialize it as a search result
    nelem += serializeResult(req, outctx, &r);
    SearchResult_Clear(&r);
    if (++nrows >= limit) {
      break;
    }
  }

  req->stateflags |= QEXEC_S_SENTONE;
  // TODO: Check for errors in `rc`

  SearchResult_Destroy(&r);
  RedisModule_ReplySetArrayLength(outctx, nelem);
}

void AREQ_Free(AggregateRequest *areq) {
}