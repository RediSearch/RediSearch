
#include "aggregate.h"
#include "reducer.h"

#include "query.h"
#include "extension.h"
#include "result_processor.h"
#include "highlight_processor.h"
#include "ext/default.h"
#include "query_error.h"

#include "util/arr.h"
#include "rmutil/util.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Ensures that the user has not requested one of the 'extended' features. Extended
 * in this case refers to reducers which re-create the search results.
 * @param areq the request
 * @param name the name of the option that requires simple mode. Used for error
 *   formatting
 * @param status the error object
 */
void AREQ::ensureSimpleMode() {
  if (reqflags & QEXEC_F_IS_EXTENDED) throw Error("Single mod test failed");
  reqflags |= QEXEC_F_IS_SEARCH;
}

//---------------------------------------------------------------------------------------------

/**
 * Like @ref ensureSimpleMode(), but does the opposite -- ensures that one of the
 * 'simple' options - i.e. ones which rely on the field to be the exact same as
 * found in the document - was not requested.
 */
int AREQ::ensureExtendedMode(const char *name, QueryError *status) {
  if (reqflags & QEXEC_F_IS_SEARCH) {
    status->SetErrorFmt(QUERY_EINVAL,
                        "option `%s` is mutually exclusive with simple (i.e. search) options",
                        name);
    return 0;
  }
  reqflags |= QEXEC_F_IS_EXTENDED;
  return 1;
}

///////////////////////////////////////////////////////////////////////////////////////////////

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, int allowLegacy);

//---------------------------------------------------------------------------------------------

ReturnedField::~ReturnedField() {
}

//---------------------------------------------------------------------------------------------

ReturnedField &FieldList::CreateField(const char *name) {
  for (auto &field: fields) {
    if (!strcasecmp(field.name.c_str(), name)) {
      return field;
    }
  }
  fields.emplace_back(name);
  return fields.back();
}

//---------------------------------------------------------------------------------------------

void FieldList::RestrictReturn() {
  if (!explicitReturn) {
    return;
  }

  for (auto it = fields.begin(); it != fields.end(); ++it) {
    if (it->explicitReturn) {
      it = fields.erase(it);
    }
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

int AREQ::parseCursorSettings(ArgsCursor *ac, QueryError *status) {
  ACArgSpec specs[] = {{name: "MAXIDLE",
                        type: AC_ARGTYPE_UINT,
                        target: &cursorMaxIdle,
                        intflags: AC_F_GE1},
                       {name: "COUNT",
                        type: AC_ARGTYPE_UINT,
                        target: &cursorChunkSize,
                        intflags: AC_F_GE1},
                       {nullptr}};

  int rv;
  ACArgSpec *errArg = nullptr;
  if ((rv = ac->ParseArgSpec(specs, &errArg)) != AC_OK && rv != AC_ERR_ENOENT) {
    QERR_MKBADARGS_AC(status, errArg->name, rv);
    return REDISMODULE_ERR;
  }

  if (cursorMaxIdle == 0 || cursorMaxIdle > RSGlobalConfig.cursorMaxIdle) {
    cursorMaxIdle = RSGlobalConfig.cursorMaxIdle;
  }
  reqflags |= QEXEC_F_IS_CURSOR;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

#define ARG_HANDLED 1
#define ARG_ERROR -1
#define ARG_UNKNOWN 0

int AREQ::handleCommonArgs(ArgsCursor *ac, bool allowLegacy, QueryError *status) {
  int rv;
  // This handles the common arguments that are not stateful
  if (ac->AdvanceIfMatch("LIMIT")) {
    PLN_ArrangeStep *arng = ap.GetOrCreateArrangeStep();
    // Parse offset, length
    if (ac->NumRemaining() < 2) {
      status->SetError(QUERY_EPARSEARGS, "LIMIT requires two arguments");
      return ARG_ERROR;
    }
    if ((rv = ac->GetU64(&arng->offset, 0)) != AC_OK ||
        (rv = ac->GetU64(&arng->limit, 0)) != AC_OK) {
      status->SetError(QUERY_EPARSEARGS, "LIMIT needs two numeric arguments");
      return ARG_ERROR;
    }

    if (arng->limit == 0) {
      // LIMIT 0 0
      reqflags |= QEXEC_F_NOROWS;
    } else if ((arng->limit > RSGlobalConfig.maxSearchResults) && (reqflags & QEXEC_F_IS_SEARCH)) {
      status->SetErrorFmt(QUERY_ELIMIT, "LIMIT exceeds maximum of %llu",
                          RSGlobalConfig.maxSearchResults);
      return ARG_ERROR;
    }
  } else if (ac->AdvanceIfMatch("SORTBY")) {
    PLN_ArrangeStep *arng = ap.GetOrCreateArrangeStep();
    if ((parseSortby(arng, ac, status, reqflags & QEXEC_F_IS_SEARCH)) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (ac->AdvanceIfMatch("ON_TIMEOUT")) {
    if (ac->NumRemaining() < 1) {
      status->SetError(QUERY_EPARSEARGS, "Need argument for ON_TIMEOUT");
      return ARG_ERROR;
    }
    const char *policystr = ac->GetStringNC(nullptr);
    tmoPolicy = TimeoutPolicy_Parse(policystr, strlen(policystr));
    if (tmoPolicy == TimeoutPolicy_Invalid) {
      status->SetErrorFmt(QUERY_EPARSEARGS, "'%s' is not a valid timeout policy",
                             policystr);
      return ARG_ERROR;
    }
  } else if (ac->AdvanceIfMatch("WITHCURSOR")) {
    if (parseCursorSettings(ac, status) != REDISMODULE_OK) {
      return ARG_ERROR;
    }
  } else if (ac->AdvanceIfMatch("_NUM_SSTRING")) {
    reqflags |= QEXEC_F_TYPED;
  } else if (ac->AdvanceIfMatch("WITHRAWIDS")) {
    reqflags |= QEXEC_F_SENDRAWIDS;
  } else {
    return ARG_UNKNOWN;
  }

  return ARG_HANDLED;
}

//---------------------------------------------------------------------------------------------

static int parseSortby(PLN_ArrangeStep *arng, ArgsCursor *ac, QueryError *status, int isLegacy) {
  // Prevent multiple SORTBY steps
  if (!arng->sortKeys.empty()) {
    QERR_MKBADARGS_FMT(status, "Multiple SORTBY steps are not allowed. Sort multiple fields in a single step");
    return REDISMODULE_ERR;
  }

  // Assume argument is at 'SORTBY'
  ArgsCursor subArgs;
  int rv;
  int legacyDesc = 0;

  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 0 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = SORTASCMAP_INIT;
  Vector<String> keys;

  if (isLegacy) {
    if (ac->NumRemaining() > 0) {
      // Mimic subArgs to contain the single field we already have
      ac->GetSlice(&subArgs, 1);
      if (ac->AdvanceIfMatch("DESC")) {
        legacyDesc = 1;
      } else if (ac->AdvanceIfMatch("ASC")) {
        legacyDesc = 0;
      }
    } else {
      goto err;
    }
  } else {
    rv = ac->GetVarArgs(&subArgs);
    if (rv != AC_OK) {
      QERR_MKBADARGS_AC(status, "SORTBY", rv);
      goto err;
    }
  }

  if (isLegacy) {
    // Legacy demands one field and an optional ASC/DESC parameter. Both
    // of these are handled above, so no need for argument parsing
    const char *s = subArgs.GetStringNC(nullptr);
    keys.push_back(s);

    if (legacyDesc) {
      SORTASCMAP_SETDESC(ascMap, 0);
    }
  } else {
    while (!subArgs.IsAtEnd()) {
      const char *s = subArgs.GetStringNC(nullptr);
      if (*s == '@') {
        if (keys.size() >= SORTASCMAP_MAXFIELDS) {
          QERR_MKBADARGS_FMT(status, "Cannot sort by more than %lu fields", SORTASCMAP_MAXFIELDS);
          goto err;
        }
        s++;
        keys.push_back(s);
        continue;
      }

      if (!strcasecmp(s, "ASC")) {
        SORTASCMAP_SETASC(ascMap, keys.size() - 1);
      } else if (!strcasecmp(s, "DESC")) {
        SORTASCMAP_SETDESC(ascMap, keys.size() - 1);
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
  if (ac->AdvanceIfMatch("MAX")) {
    unsigned mx = 0;
    if ((rv = ac->GetUnsigned( &mx, 0) != AC_OK)) {
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
  return REDISMODULE_ERR;
}

//---------------------------------------------------------------------------------------------

static int parseQueryLegacyArgs(ArgsCursor *ac, RSSearchOptions *options, QueryError *status) {
  if (ac->AdvanceIfMatch("FILTER")) {
    try {
      options->legacy.filters.push_back(new NumericFilter(ac, status));
    } catch (Error &x) {
      return ARG_ERROR;
    }
  } else if (ac->AdvanceIfMatch("GEOFILTER")) {
    try {
      options->legacy.gf = new GeoFilter(ac, status);
    } catch (Error &x) {
      return ARG_ERROR;
    }
  } else {
    return ARG_UNKNOWN;
  }
  return ARG_HANDLED;
}

//---------------------------------------------------------------------------------------------

int AREQ::parseQueryArgs(ArgsCursor *ac, RSSearchOptions *searchOpts, AggregatePlan *plan,
                         QueryError *status) {
  // Parse query-specific arguments..
  const char *languageStr = nullptr;
  ArgsCursor returnFields;
  ArgsCursor inKeys;
  ArgsCursor inFields;
  ACArgSpec querySpecs[] = {
      {name: "INFIELDS", type: AC_ARGTYPE_SUBARGS, target: &inFields},  // Comment
      {name: "SLOP",     type: AC_ARGTYPE_INT,     target: &searchOpts->slop, intflags: AC_F_COALESCE},
      {name: "LANGUAGE", type: AC_ARGTYPE_STRING,  target: &languageStr},
      {name: "EXPANDER", type: AC_ARGTYPE_STRING,  target: &searchOpts->expanderName},
      {name: "INKEYS",   type: AC_ARGTYPE_SUBARGS, target: &inKeys},
      {name: "SCORER",   type: AC_ARGTYPE_STRING,  target: &searchOpts->scorerName},
      {name: "RETURN",   type: AC_ARGTYPE_SUBARGS, target: &returnFields},
      {AC_MKBITFLAG("INORDER", &searchOpts->flags, Search_InOrder)},
      {AC_MKBITFLAG("VERBATIM", &searchOpts->flags, Search_Verbatim)},
      {AC_MKBITFLAG("WITHSCORES", &reqflags, QEXEC_F_SEND_SCORES)},
      {AC_MKBITFLAG("WITHSORTKEYS", &reqflags, QEXEC_F_SEND_SORTKEYS)},
      {AC_MKBITFLAG("WITHPAYLOADS", &reqflags, QEXEC_F_SEND_PAYLOADS)},
      {AC_MKBITFLAG("NOCONTENT", &reqflags, QEXEC_F_SEND_NOFIELDS)},
      {AC_MKBITFLAG("NOSTOPWORDS", &searchOpts->flags, Search_NoStopwrods)},
      {AC_MKBITFLAG("EXPLAINSCORE", &reqflags, QEXEC_F_SEND_SCOREEXPLAIN)},
      {name: "PAYLOAD", type: AC_ARGTYPE_BUFFER, target: &ast->payload},
      {0}};

  while (!ac->IsAtEnd()) {
    ACArgSpec *errSpec = nullptr;
    int rv = ac->ParseArgSpec(querySpecs, &errSpec);
    if (rv == AC_OK) {
      continue;
    }

    if (rv != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errSpec->name, rv);
      return REDISMODULE_ERR;
    }

    // See if this is one of our arguments which requires special handling
    if (ac->AdvanceIfMatch("SUMMARIZE")) {
      ensureSimpleMode();
      outFields.ParseSummarize(ac);
      reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if (ac->AdvanceIfMatch("HIGHLIGHT")) {
      ensureSimpleMode();
      outFields.ParseHighlight(ac);
      reqflags |= QEXEC_F_SEND_HIGHLIGHT;

    } else if ((reqflags & QEXEC_F_IS_SEARCH) &&
               ((rv = parseQueryLegacyArgs(ac, searchOpts, status)) != ARG_UNKNOWN)) {
      if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      }
    } else {
      int rv = handleCommonArgs(ac, true, status);
      if (rv == ARG_HANDLED) {
        // nothing
      } else if (rv == ARG_ERROR) {
        return REDISMODULE_ERR;
      } else {
        break;
      }
    }
  }

  if ((reqflags & QEXEC_F_SEND_SCOREEXPLAIN) && !(reqflags & QEXEC_F_SEND_SCORES)) {
    QERR_MKBADARGS_FMT(status, "EXPLAINSCORE must be accompanied with WITHSCORES");
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < inKeys.argc; ++i) {
    searchOpts->inkeys.push_back((const char *) inKeys.objs[i]);
  }

  for (int i = 0; i < inFields.argc; ++i) {
    searchOpts->inkeys.push_back((const char *) inFields.objs[i]);
  }

  searchOpts->language = RSLanguage_Find(languageStr);

  if (returnFields.IsInitialized()) {
    ensureSimpleMode();

    outFields.explicitReturn = true;
    if (returnFields.argc == 0) {
      reqflags |= QEXEC_F_SEND_NOFIELDS;
    }

    while (!returnFields.IsAtEnd()) {
      const char *name = returnFields.GetStringNC(nullptr);
      outFields.CreateField(name).explicitReturn = true;
    }
  }

  outFields.RestrictReturn();
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

char *PLN_Reducer::getAlias(const char *func) {
  sds out = sdsnew("__generated_alias");
  out = sdscat(out, func);
  // only put parentheses if we actually have args
  char buf[255];
  ArgsCursor tmp = args;
  while (!tmp.IsAtEnd()) {
    size_t l;
    const char *s = tmp.GetStringNC(&l);
    while (*s == '@') {
      // Don't allow the leading '@' to be included as an alias!
      ++s;
      --l;
    }
    out = sdscatlen(out, s, l);
    if (!tmp.IsAtEnd()) {
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

//---------------------------------------------------------------------------------------------

PLN_Reducer::PLN_Reducer(const char *name_, const ArgsCursor *ac) {
  name = name_; //@@ owership
  int rv = ac->GetVarArgs(&args);
  if (rv != AC_OK) throw BadArgsError(rv, name);

  const char *_alias = nullptr;
  // See if there is an alias
  if (ac->AdvanceIfMatch("AS")) {
    rv = ac->GetString(&_alias, nullptr, 0);
    if (rv != AC_OK) {
      throw BadArgsError(rv, "AS");
    }
  }
  if (_alias == nullptr) {
    alias = getAlias(name);
  } else {
    alias = rm_strdup(_alias);
  }
}

//---------------------------------------------------------------------------------------------

PLN_Reducer::~PLN_Reducer() {
  rm_free(alias);
}

//---------------------------------------------------------------------------------------------

/**
 * Adds a reducer (with its arguments) to the group step
 * @param gstp the group step
 * @param name the name of the reducer
 * @param ac arguments to the reducer; if an alias is used, it is provided here as well.
 */

int PLN_GroupStep::AddReducer(const char *name, ArgsCursor *ac, QueryError *status) {
  try {
    PLN_Reducer gr(name, ac);
    reducers.push_back(gr);
  } catch (const QueryError &x) {
    *status = x;
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

PLN_BaseStep::~PLN_BaseStep() {
  if (alias) {
    //rm_free((void *)alias);
  }
}

//---------------------------------------------------------------------------------------------

PLN_GroupStep::PLN_GroupStep(const char **properties_, size_t nproperties_) : PLN_BaseStep(PLN_T_GROUP) {
  properties = properties_;
  nproperties = nproperties_;
}

//---------------------------------------------------------------------------------------------

int AREQ::parseGroupby(ArgsCursor *ac, QueryError *status) {
  ArgsCursor groupArgs;
  const char *s;
  ac->GetString(&s, nullptr, AC_F_NOADVANCE);
  int rv = ac->GetVarArgs(&groupArgs);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "GROUPBY", rv);
    return REDISMODULE_ERR;
  }

  // Number of fields.. now let's see the reducers
  PLN_GroupStep *gstp = new PLN_GroupStep((const char **)groupArgs.objs, groupArgs.argc);
  ap.AddStep(gstp);

  while (ac->AdvanceIfMatch("REDUCE")) {
    const char *name;
    if (ac->GetString(&name, nullptr, 0) != AC_OK) {
      QERR_MKBADARGS_AC(status, "REDUCE", rv);
      return REDISMODULE_ERR;
    }
    if (gstp->AddReducer(name, ac, status) != REDISMODULE_OK) {
      goto error;
    }
  }
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

//---------------------------------------------------------------------------------------------

PLN_MapFilterStep::~PLN_MapFilterStep() {
  delete parsedExpr;
  if (shouldFreeRaw) {
    rm_free((char *)rawExpr);
  }
}

//---------------------------------------------------------------------------------------------

PLN_MapFilterStep::PLN_MapFilterStep(const char *expr, int mode) : PLN_BaseStep(mode) {
  rawExpr = expr;
}

//---------------------------------------------------------------------------------------------

int AREQ::handleApplyOrFilter(ArgsCursor *ac, bool isApply, QueryError *status) {
  // Parse filters!
  const char *expr = nullptr;
  int rv = ac->GetString(&expr, nullptr, 0);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "APPLY/FILTER", rv);
    return REDISMODULE_ERR;
  }

  PLN_MapFilterStep *stp = new PLN_MapFilterStep(expr, isApply ? PLN_T_APPLY : PLN_T_FILTER);
  ap.AddStep(stp);

  if (isApply) {
    if (ac->AdvanceIfMatch("AS")) {
      const char *alias;
      if (ac->GetString(&alias, nullptr, 0) != AC_OK) {
        QERR_MKBADARGS_FMT(status, "AS needs argument");
        goto error;
      }
      stp->alias = rm_strdup(alias);
    } else {
      stp->alias = rm_strdup(expr);
    }
  }
  return REDISMODULE_OK;

error:
  if (stp) {
    ap.PopStep();
    delete stp;
  }
  return REDISMODULE_ERR;
}

//---------------------------------------------------------------------------------------------

PLN_LoadStep::PLN_LoadStep(ArgsCursor &fields) : PLN_BaseStep(PLN_T_LOAD) {
  args = fields;
  keys = rm_calloc(fields.argc, sizeof(*keys));
}

//---------------------------------------------------------------------------------------------

PLN_LoadStep::~PLN_LoadStep() {
  rm_free(keys);
}

//---------------------------------------------------------------------------------------------

int AREQ::handleLoad(ArgsCursor *ac, QueryError *status) {
  ArgsCursor loadfields;
  int rc = ac->GetVarArgs(&loadfields);
  if (rc != AC_OK) {
    QERR_MKBADARGS_AC(status, "LOAD", rc);
    return REDISMODULE_ERR;
  }

  PLN_LoadStep *lstp = new PLN_LoadStep(loadfields);
  ap.AddStep(lstp);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

/**
 * Create a new aggregate request. The request's lifecycle consists of several
 * stages:
 *
 * 1) New - creates a blank request
 *
 * 2) Compile - this gathers the request options from the commandline, creates
 *    the basic abstract plan.
 *
 * 3) ApplyContext - This is the second stage of Compile, and applies
 *    a stateful context. The reason for this state remaining separate is
 *    the ability to test parsing and option logic without having to worry
 *    that something might touch the underlying index.
 *    Compile also provides a place to optimize or otherwise rework the plan
 *    based on information known only within the query itself.
 *
 * 4) BuildPipeline: This lines up all the iterators so that it can be read from.
 *
 * 5) Execute: This step is optional, and iterates through the result iterator,
 *    formatting the output and sending it to the network client. This step is
 *    optional, since the iterator can be obtained directly via AREQ::RP and processed directly.
 *
 * 6) Free: This releases all resources consumed by the request
 */

//---------------------------------------------------------------------------------------------

/**
 * Compile the request given the arguments. This does not rely on
 * Redis-specific states and may be unit-tested. This largely just
 * compiles the options and parses the commands..
 */

int AREQ::Compile(RedisModuleString **argv, int argc, QueryError *status) {
  args = rm_malloc(sizeof(*args) * argc);
  nargs = argc;
  for (size_t ii = 0; ii < argc; ++ii) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(argv[ii], &n);
    args[ii] = sdsnewlen(s, n);
  }

  // Parse the query and basic keywords first..
  ArgsCursor ac;
  ac.InitSDS(args, nargs);

  if (ac.IsAtEnd()) {
    status->SetError(QUERY_EPARSEARGS, "No query string provided");
    return REDISMODULE_ERR;
  }

  query = ac.GetStringNC(nullptr);

  if (parseQueryArgs(&ac, &searchopts, &ap, status) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  int hasLoad = 0;

  // Now we have a 'compiled' plan. Let's get some more options..

  while (!ac.IsAtEnd()) {
    int rv = handleCommonArgs(&ac, !!(reqflags & QEXEC_F_IS_SEARCH), status);
    if (rv == ARG_HANDLED) {
      continue;
    } else if (rv == ARG_ERROR) {
      return REDISMODULE_ERR;
    }

    if (ac.AdvanceIfMatch("GROUPBY")) {
      if (!ensureExtendedMode("GROUPBY", status)) {
        return REDISMODULE_ERR;
      }
      if (parseGroupby(&ac, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (ac.AdvanceIfMatch("APPLY")) {
      if (handleApplyOrFilter(&ac, true, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (ac.AdvanceIfMatch("LOAD")) {
      if (handleLoad(&ac, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (ac.AdvanceIfMatch("FILTER")) {
      if (handleApplyOrFilter(&ac, false, status) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else {
      status->FmtUnknownArg(&ac, "<main>");
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void QueryAST::applyGlobalFilters(RSSearchOptions &opts, const RedisSearchCtx &sctx) {
  // The following blocks will set filter options on the entire query
  for (auto filter: opts.legacy.filters) {
    SetGlobalFilters(filter);
  }

  if (opts.legacy.gf) {
    SetGlobalFilters(opts.legacy.gf);
  }

  for (auto inkey: opts.inkeys) {
    t_docId did{sctx.spec->docs.GetId(inkey)};
    if (!!did) {
      opts.inids.push_back(did);
    }
  }
  if (!opts.inids.empty()) {
    SetGlobalFilters(opts.inids);
  }
}

//---------------------------------------------------------------------------------------------

/**
 * This stage will apply the context to the request. During this phase, the
 * query will be parsed (and matched according to the schema), and the reducers
 * will be loaded and analyzed.
 *
 * This consumes a refcount of the context used.
 *
 * Note that this function consumes a refcount even if it fails!
 */

int AREQ::ApplyContext(QueryError *status) {
  // Sort through the applicable options:
  IndexSpec *index = sctx->spec;
  RSSearchOptions &opts = searchopts;

  if ((index->flags & Index_StoreByteOffsets) == 0 && (reqflags & QEXEC_F_SEND_HIGHLIGHT)) {
    status->SetError(QUERY_EINVAL,
                     "Cannot use highlight/summarize because NOOFSETS was specified at index level");
    return REDISMODULE_ERR;
  }

  // Go through the query options and see what else needs to be filled in!
  // 1) INFIELDS
  if (!opts.legacy.infields.empty()) {
    opts.fieldmask = 0;
    for (auto infield: opts.legacy.infields) {
      t_fieldMask bit = index->GetFieldBit(infield);
      opts.fieldmask |= bit;
    }
  }

  if (opts.language == RS_LANG_UNSUPPORTED) {
    status->SetError(QUERY_EINVAL, "No such language");
    return REDISMODULE_ERR;
  }

  if (opts.scorerName) {
    try {
      g_ext.GetScorer(opts.scorerName);
    } catch (Error &x) {
      status->SetErrorFmt(QUERY_EINVAL, "No such scorer %s", opts.scorerName);
      return REDISMODULE_ERR;
    }
  }

  if (!(opts.flags & Search_NoStopwrods)) {
    opts.stopwords = sctx->spec->stopwords;
  }

  try {
    ast = std::make_unique<QueryAST>(*sctx, searchopts, query, status);
    ast->applyGlobalFilters(opts, *sctx);
  } catch (Error &x) {
    return REDISMODULE_ERR;
  }

  if (!(opts.flags & Search_Verbatim)) {
    if (ast->Expand(opts.expanderName, &opts, *sctx, status) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  conc = std::make_unique<ConcurrentSearch>(sctx->redisCtx);
  rootiter = ast->Iterate(opts, *sctx, conc.get());
  if (!rootiter) throw Error("QAST_Iterate failed");

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

ResultProcessor *PLN_GroupStep::buildRP(RLookup *srclookup, QueryError *err) {
  const RLookupKey *srckeys[nproperties], *dstkeys[nproperties];
  for (size_t ii = 0; ii < nproperties; ++ii) {
    const char *fldname = properties[ii] + 1;  // account for the @-
    srckeys[ii] = srclookup->GetKey(fldname, RLOOKUP_F_NOINCREF);
    if (!srckeys[ii]) {
      err->SetErrorFmt(QUERY_ENOPROPKEY, "No such property `%s`", fldname);
      return nullptr;
    }
    dstkeys[ii] = lookup->GetKey(fldname, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
  }

  Grouper *grp = new Grouper(srckeys, dstkeys, nproperties);

  for (auto pr : reducers) {
    // Build the actual reducer
    ReducerOptions options = REDUCEROPTS_INIT(pr.name, &pr.args, srclookup, err);
    ReducerFactory ff = RDCR_GetFactory(pr.name);
    if (!ff) {
      // No such reducer!
      delete grp;
      err->SetErrorFmt(QUERY_ENOREDUCER, "No such reducer: %s", pr.name);
      return nullptr;
    }
    Reducer *rr = ff(&options);
    if (!rr) {
      delete grp;
      return nullptr;
    }

    // Set the destination key for the grouper!
    RLookupKey *dstkey = lookup->GetKey(pr.alias, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
    grp->AddReducer(rr, dstkey);
  }

  return grp;
}

//---------------------------------------------------------------------------------------------

/** Pushes a processor up the stack. Returns the newly pushed processor
 * @param rp the processor to push
 * @param rpUpstream previous processor (used as source for rp)
 * @return the processor passed in `rp`.
 */
ResultProcessor *AREQ::pushRP(ResultProcessor *rp, ResultProcessor *rpUpstream) {
  rp->upstream = rpUpstream;
  rp->parent = &*qiter;
  qiter->endProc = rp;
  return rp;
}

//---------------------------------------------------------------------------------------------

ResultProcessor *AREQ::getGroupRP(PLN_GroupStep *gstp, ResultProcessor *rpUpstream,
                                  QueryError *status) {
  AGGPlan *pln = &ap;
  RLookup *lookup = pln->GetLookup(gstp, AGPLN_GETLOOKUP_PREV);
  ResultProcessor *groupRP = gstp->buildRP(lookup, status);

  if (!groupRP) {
    return nullptr;
  }

  // See if we need a LOADER group here...?
  RLookup *firstLk = pln->GetLookup(gstp, AGPLN_GETLOOKUP_FIRST);

  if (firstLk == lookup) {
    // See if we need a loader step?
    const RLookupKey **kklist = nullptr;
    for (RLookupKey *kk = firstLk->head; kk; kk = kk->next) {
      if ((kk->flags & RLOOKUP_F_DOCSRC) && (!(kk->flags & RLOOKUP_F_SVSRC))) {
        *array_ensure_tail(&kklist, const RLookupKey *) = kk;
      }
    }
    if (kklist != nullptr) {
      ResultProcessor *loader = new ResultsLoader(firstLk, kklist, array_len(kklist));
      array_free(kklist);
      if (!loader) throw Error("Failed to create ResultsLoader");
      rpUpstream = pushRP(loader, rpUpstream);
    }
  }

  return pushRP(groupRP, rpUpstream);
}

//---------------------------------------------------------------------------------------------

#define DEFAULT_LIMIT 10

ResultProcessor *AREQ::getArrangeRP(AGGPlan *pln, PLN_ArrangeStep &astp, ResultProcessor *up,
                                    QueryError *status) {
  ResultProcessor *rp = nullptr;

  size_t limit = astp.offset + astp.limit;
  if (!limit) {
    limit = DEFAULT_LIMIT;
  }

  if (!astp.sortKeys.empty()) {
    RLookup *lk = pln->GetLookup(&astp, AGPLN_GETLOOKUP_PREV);

    //@@ move to PLN_ArrangeStep class
    for (const auto &key: astp.sortKeys) {
      auto rlkey = lk->GetKey(key, RLOOKUP_F_NOINCREF);
      if (!rlkey) {
        status->SetErrorFmt(QUERY_ENOPROPKEY, "Property `%s` not loaded nor in schema", key.c_str());
        return nullptr;
      }
      astp.sortkeysLK.push_back(rlkey);
    }

    rp = new RPSorter(limit, astp.sortkeysLK, astp.sortAscMap);
    up = pushRP(rp, up);
  }

  // No sort? then it must be sort by score, which is the default.
  if (rp == nullptr && (reqflags & QEXEC_F_IS_SEARCH)) {
    rp = new RPSorter(limit);
    up = pushRP(rp, up);
  }

  if (astp.offset || (astp.limit && !rp)) {
    rp = new RPPager(astp.offset, astp.limit);
    up = pushRP(rp, up);
  }

  return rp;
}

//---------------------------------------------------------------------------------------------

ResultProcessor *AREQ::getScorerRP() {
  const char *scorer_name = searchopts.scorerName;
  if (!scorer_name) {
    scorer_name = DEFAULT_SCORER_NAME;
  }
  Scorer scorer = g_ext.GetScorer(scorer_name);
  if (!scorer) throw Error("Invalid scorer: %s", scorer_name);
  ScorerArgs scargs{*sctx->spec, ast->payload, !!(reqflags & QEXEC_F_SEND_SCOREEXPLAIN)};
  if (reqflags & QEXEC_F_SEND_SCOREEXPLAIN) {
    scargs.explain = new ScoreExplain();
  }
  scargs.indexStats = sctx->spec->stats;
  scargs.payload = ast->payload;
  return new RPScorer(&scorer, &scargs);
}

//---------------------------------------------------------------------------------------------

bool AGGPlan::hasQuerySortby() const {
  const PLN_BaseStep *bstp = FindStep(nullptr, nullptr, PLN_T_GROUP);
  if (bstp != nullptr) {
    const PLN_ArrangeStep *arng = FindStep(nullptr, bstp, PLN_T_ARRANGE);
    if (arng && !arng->sortKeys.empty()) {
      return true;
    }
  } else {
    // no group... just see if we have an arrange step
    const PLN_ArrangeStep *arng = FindStep(nullptr, nullptr, PLN_T_ARRANGE);
    return arng && !arng->sortKeys.empty();
  }
  return false;
}

//---------------------------------------------------------------------------------------------

#define PUSH_RP() \
  do { \
    rpUpstream = pushRP(rp, rpUpstream); \
    rp = nullptr; \
  } while(0)

//---------------------------------------------------------------------------------------------

// Builds the implicit pipeline for querying and scoring, and ensures that our
// subsequent execution stages actually have data to operate on.

void AREQ::buildImplicitPipeline(QueryError *status) {
  qiter = std::make_unique<QueryIterator>();
  qiter->conc = &*conc;
  qiter->sctx = &*sctx;
  qiter->err = status;

  std::shared_ptr<IndexSpecFields> cache = sctx->spec->GetSpecCache();
  RS_LOG_ASSERT(cache, "IndexSpec::GetSpecCache failed")

  RLookup *first = ap.GetLookup(nullptr, AGPLN_GETLOOKUP_FIRST);
  if (first) {
    first->Reset(cache);
  }

  ResultProcessor *rp = new RPIndexIterator(rootiter);
  ResultProcessor *rpUpstream = nullptr;
  qiter->rootProc = qiter->endProc = rp;
  PUSH_RP();

  // Create a scorer if there is no subsequent sorter within this grouping
  if (!ap.hasQuerySortby() && (reqflags & QEXEC_F_IS_SEARCH)) {
    rp = getScorerRP();
    PUSH_RP();
  }
}

//---------------------------------------------------------------------------------------------

// This handles the RETURN and SUMMARIZE keywords, which operate on the result
// which is about to be returned. It is only used in FT.SEARCH mode

int AREQ::buildOutputPipeline(QueryError *status) {
  AGGPlan &pln = ap;
  ResultProcessor *rp = nullptr, *rpUpstream = qiter->endProc;

  RLookup *lookup = pln.GetLookup(nullptr, AGPLN_GETLOOKUP_LAST);
  // Add a LOAD step...
  const RLookupKey **loadkeys = nullptr;
  if (outFields.explicitReturn) {
    // Go through all the fields and ensure that each one exists in the lookup stage
    for (auto &field: outFields.fields) {
      RLookupKey *lk = lookup->GetKey(field.name, RLOOKUP_F_NOINCREF | RLOOKUP_F_OCREAT);
      if (!lk) {
        // TODO: this is a dead code
        status->SetErrorFmt(QUERY_ENOPROPKEY, "Property '%s' not loaded or in schema", field.name);
        goto error;
      }
      *array_ensure_tail(&loadkeys, const RLookupKey *) = lk;
      // assign explicit output flag
      lk->flags |= RLOOKUP_F_EXPLICITRETURN;
    }
  }
  rp = new ResultsLoader(lookup, loadkeys, loadkeys ? array_len(loadkeys) : 0);
  if (loadkeys) {
    array_free(loadkeys);
  }
  PUSH_RP();

  if (reqflags & QEXEC_F_SEND_HIGHLIGHT) {
    RLookup *lookup = pln.GetLookup(nullptr, AGPLN_GETLOOKUP_LAST);
    for (auto &field: outFields.fields) {
      RLookupKey *kk = lookup->GetKey(field.name, 0);
      if (!kk) {
        status->SetErrorFmt(QUERY_ENOPROPKEY, "No such property `%s`", field.name);
        goto error;
      } else if (!(kk->flags & (RLOOKUP_F_DOCSRC | RLOOKUP_F_SVSRC))) {
        // TODO: this is a dead code
        status->SetErrorFmt(QUERY_EINVAL, "Property `%s` is not in document", field.name);
        goto error;
      }
      field.lookupKey = kk;
    }
    rp = new Highlighter(&searchopts, outFields, lookup);
    PUSH_RP();
  }

  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

//---------------------------------------------------------------------------------------------

// Constructs the pipeline objects needed to actually start processing the requests.
// This does not yet start iterating over the objects

int AREQ::BuildPipeline(BuildPipelineOptions options, QueryError *status) {
  if (!(options & AREQ_BUILDPIPELINE_NO_ROOT)) {
    buildImplicitPipeline(status);
  }

  AGGPlan *pln = &ap;
  ResultProcessor *rp = nullptr, *rpUpstream = qiter->endProc;

  // Whether we've applied a SORTBY yet..
  int hasArrange = 0;

  for (PLN_BaseStep *step = pln->steps.front(); step != pln->steps.back(); step = step->NextStep()) {
    switch (step->type) {
      case PLN_T_GROUP: {
        rpUpstream = getGroupRP((PLN_GroupStep *)step, rpUpstream, status);
        if (!rpUpstream) {
          goto error;
        }
        break;
      }

      case PLN_T_ARRANGE: {
        auto astp = *dynamic_cast<PLN_ArrangeStep*>(step);
        rp = getArrangeRP(pln, astp, rpUpstream, status);
        if (!rp) {
          goto error;
        }
        hasArrange = 1;
        rpUpstream = rp;
        break;
      }

      case PLN_T_APPLY:
      case PLN_T_FILTER: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)step;
        // Ensure the lookups can actually find what they need
        RLookup *curLookup = pln->GetLookup(step, AGPLN_GETLOOKUP_PREV);
        mstp->parsedExpr = RSExpr::ParseAST(mstp->rawExpr, strlen(mstp->rawExpr), status);
        if (!mstp->parsedExpr) {
          goto error;
        }

        if (!mstp->parsedExpr->GetLookupKeys(curLookup, status)) {
          goto error;
        }

        if (step->type == PLN_T_APPLY) {
          RLookupKey *dstkey =
              curLookup->GetKey(step->alias, RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF);
          rp = new RPProjector(mstp->parsedExpr, curLookup, dstkey);
        } else {
          rp = new RPFilter(mstp->parsedExpr, curLookup);
        }
        PUSH_RP();
        break;
      }

      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)step;
        RLookup *curLookup = pln->GetLookup(step, AGPLN_GETLOOKUP_PREV);
        RLookup *rootLookup = pln->GetLookup(nullptr, AGPLN_GETLOOKUP_FIRST);
        if (curLookup != rootLookup) {
          status->SetError(QUERY_EINVAL,
                              "LOAD cannot be applied after projectors or reducers");
          goto error;
        }
        // Get all the keys for this lookup...
        while (!lstp->args.IsAtEnd()) {
          const char *s = lstp->args.GetStringNC(nullptr);
          if (*s == '@') {
            s++;
          }
          const RLookupKey *kk = curLookup->GetKey(s, RLOOKUP_F_OEXCL | RLOOKUP_F_OCREAT);
          if (!kk) {
            // We only get a nullptr return if the key already exists, which means
            // that we don't need to retrieve it again.
            continue;
          }
          lstp->keys[lstp->nkeys++] = kk;
        }
        if (lstp->nkeys) {
          rp = new ResultsLoader(curLookup, lstp->keys, lstp->nkeys);
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
        abort();
    }
  }

  // If no LIMIT or SORT has been applied, do it somewhere here so we don't
  // return the entire matching result set!
  if (!hasArrange && (reqflags & QEXEC_F_IS_SEARCH)) {
    PLN_ArrangeStep astp;
    rp = getArrangeRP(pln, astp, rpUpstream, status);
    if (!rp) {
      goto error;
    }
    rpUpstream = rp;
  }

  // If this is an FT.SEARCH command which requires returning of some of the
  // document fields, handle those options in this function
  if ((reqflags & QEXEC_F_IS_SEARCH) && !(reqflags & QEXEC_F_SEND_NOFIELDS)) {
    if (buildOutputPipeline(status) != REDISMODULE_OK) {
      goto error;
    }
  }

  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

//---------------------------------------------------------------------------------------------

AREQ::~AREQ() {
  // First, free the result processors
  ResultProcessor *rp = qiter->endProc;
  while (rp) {
    ResultProcessor *next = rp->upstream;
    delete rp;
    rp = next;
  }

  if (rootiter) {
    delete rootiter;
    rootiter = nullptr;
  }

  // Finally, free the context.
  // If we are a cursor, some more cleanup is required since we also now own the
  // detached ("Thread Safe") context.
  RedisModuleCtx *thctx = nullptr;
  if (sctx) {
    if (reqflags & QEXEC_F_IS_CURSOR) {
      thctx = sctx->redisCtx;
      sctx->redisCtx = nullptr;
    }
  }

  for (size_t ii = 0; ii < nargs; ++ii) {
    sdsfree(args[ii]);
  }

  if (thctx) {
    RedisModule_FreeThreadSafeContext(thctx);
  }

  rm_free(args);
}

///////////////////////////////////////////////////////////////////////////////////////////////
