#include "search_request.h"
#include "aggregate/aggregate.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "stemmer.h"
#include "ext/default.h"
#include "extension.h"
#include "query.h"
#include "concurrent_ctx.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "summarize_spec.h"
#include "query_plan.h"
#include <sys/param.h>
#include <rmutil/cmdparse.h>
#include <rmutil/args.h>
#include <err.h>
#include <assert.h>

#undef SET_ERR
#undef FMT_ERR
#define SET_ERR(status, s) QueryError_SetError(status, QUERY_EKEYWORD, s)
#define FMT_ERR(unused, s, ...) QueryError_SetErrorFmt(status, QUERY_EKEYWORD, ##__VA_ARGS__)

/**
 * Handler signature. argv and argc are the original arguments passed (for the
 * entire query). offset contains the index of the keyword for this handler.
 * The first argument to the keyword will be at argv[*offset + 1].
 *
 * On return, offset should contain the index of the first token *not* handled
 * by the handler.
 *
 * The function should return REDISMODULE_{OK,ERR} as a status.
 * errStr assignment is optional. If errStr is not assigned, the default
 * handler's errStr (i.e. KeywordHandler::errStr) will be used instead.
 */
typedef int (*KeywordParser)(ArgsCursor *ac, RSSearchRequest *req, RSSearchOptions *opts,
                             RedisSearchCtx *sctx, QueryError *status);

typedef struct {
  // The keyword this parser handles
  const char *keyword;

  // Static error string to use
  const char *errStr;

  // Minimum number of arguments required. This number does not count the
  // actual keyword itself (unlike the `offset` parameter passed to the
  // handler)
  size_t minArgs;

  // The actual parser
  KeywordParser parser;
} KeywordHandler;

#define KEYWORD_HANDLER(name)                                                  \
  static int name(ArgsCursor *ac, RSSearchRequest *req, RSSearchOptions *opts, \
                  RedisSearchCtx *sctx, QueryError *status)

KEYWORD_HANDLER(parseLimit) {
  long long tmpLimit, tmpOffset;
  if (RMUtil_ParseArgs((RedisModuleString **)ac->objs, ac->argc, ac->offset, "ll", &tmpOffset,
                       &tmpLimit) != REDISMODULE_OK) {
    // printf("Couldn't parse limit\n");
    return REDISMODULE_ERR;
  }

  if (tmpLimit < 0 || tmpOffset < 0 || tmpOffset + tmpLimit > SEARCH_REQUEST_RESULTS_MAX) {
    SET_ERR(status, "LIMIT: Limit or offset too large");
    return REDISMODULE_ERR;
  }
  opts->num = tmpLimit;
  opts->offset = tmpOffset;
  AC_AdvanceBy(ac, 2);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseInfields) {
  ArgsCursor infieldsAC;
  if (AC_GetVarArgs(ac, &infieldsAC)) {
    return REDISMODULE_ERR;
  }
  opts->fieldMask =
      IndexSpec_ParseFieldMask(sctx->spec, (RedisModuleString **)infieldsAC.objs, infieldsAC.argc);
  RedisModule_Log(sctx->redisCtx, "debug", "Parsed field mask: 0x%x", opts->fieldMask);
  return REDISMODULE_OK;
}

// {field} {min} {max}
KEYWORD_HANDLER(parseNumericFilter) {
  if (req->numericFilters == NULL) {
    req->numericFilters = NewVector(NumericFilter *, 2);
  }

  NumericFilter *flt = ParseNumericFilter(sctx, (RedisModuleString **)ac->objs + ac->offset, 3);
  if (flt == NULL) {
    return REDISMODULE_ERR;
  }

  Vector_Push(req->numericFilters, flt);
  AC_AdvanceBy(ac, 3);
  // printf("Parsed filter: %f, %f\n", flt->min, flt->max);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseGeoFilter) {
  if (!req->geoFilter) {
    req->geoFilter = malloc(sizeof(*req->geoFilter));
  }

  int rv = GeoFilter_Parse(req->geoFilter, (RedisModuleString **)ac->objs + ac->offset, 5);
  AC_AdvanceBy(ac, 5);
  return rv;
}

KEYWORD_HANDLER(parseSlop) {
  if (AC_GetInt(ac, &opts->slop, AC_F_COALESCE)) {
    return REDISMODULE_ERR;
  }
  opts->flags |= Search_HasSlop;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseLanguage) {
  // make sure we search for "language" only after the query
  const char *lang = AC_GetStringNC(ac, NULL);
  if (!IsSupportedLanguage(lang, strlen(lang))) {
    return REDISMODULE_ERR;
  }
  opts->language = strdup(lang);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseExpander) {
  opts->expander = strdup(AC_GetStringNC(ac, NULL));
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handlePayload) {
  const char *payload = AC_GetStringNC(ac, &req->payload.len);
  if (req->payload.len) {
    req->payload.data = malloc(req->payload.len);
    memcpy(req->payload.data, payload, req->payload.len);
  }
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleScorer) {
  opts->scorer = strdup(AC_GetStringNC(ac, NULL));
  if (Extensions_GetScoringFunction(NULL, opts->scorer) == NULL) {
    SET_ERR(status, "Invalid scorer name");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleSummarize) {
  // Quite unsafe; need to refactor
  size_t curOffset = ac->offset;
  int rv = ParseSummarize((RedisModuleString **)ac->objs, ac->argc, &curOffset, &opts->fields);
  ac->offset = curOffset;
  return rv;
}

KEYWORD_HANDLER(handleHighlight) {
  size_t curOffset = ac->offset;
  int rv = ParseHighlight((RedisModuleString **)ac->objs, ac->argc, &curOffset, &opts->fields);
  ac->offset = curOffset;
  return rv;
}

KEYWORD_HANDLER(handleSortBy) {
  RSSortingKey sortKey;
  size_t curOffset = ac->offset;
  int rc = RSSortingTable_ParseKey(sctx->spec->sortables, &sortKey, (RedisModuleString **)ac->objs,
                                   ac->argc, &curOffset);
  if (rc == REDISMODULE_OK) {
    opts->sortBy = malloc(sizeof(sortKey));
    *opts->sortBy = sortKey;
  }
  ac->offset = curOffset;
  return rc;
}

KEYWORD_HANDLER(handleInkeys) {
  ArgsCursor keyAC;
  if (AC_GetVarArgs(ac, &keyAC)) {
    return REDISMODULE_ERR;
  }
  req->idFilter = NewIdFilter((RedisModuleString **)keyAC.objs, keyAC.argc, &sctx->spec->docs);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleReturn) {
  ArgsCursor fieldsAC;
  if (AC_GetVarArgs(ac, &fieldsAC)) {
    return REDISMODULE_ERR;
  }
  if (fieldsAC.argc == 0) {
    opts->flags |= Search_NoContent;
  }

  RedisModuleString **vargs = (RedisModuleString **)fieldsAC.objs;
  for (size_t ii = 0; ii < fieldsAC.argc; ++ii) {
    ReturnedField *field = FieldList_GetCreateField(&opts->fields, vargs[ii]);
    field->explicitReturn = 1;
  }
  opts->fields.explicitReturn = 1;
  return REDISMODULE_OK;
}

#define HANDLER_ENTRY(name, parser_, minArgs_)               \
  {                                                          \
    .keyword = name, .parser = parser_, .minArgs = minArgs_, \
    .errStr = "Bad argument for `" name "`"                  \
  }

static const KeywordHandler keywordHandlers_g[] = {HANDLER_ENTRY("LIMIT", parseLimit, 2),
                                                   HANDLER_ENTRY("INFIELDS", parseInfields, 1),
                                                   HANDLER_ENTRY("FILTER", parseNumericFilter, 3),
                                                   HANDLER_ENTRY("GEOFILTER", parseGeoFilter, 5),
                                                   HANDLER_ENTRY("SLOP", parseSlop, 1),
                                                   HANDLER_ENTRY("LANGUAGE", parseLanguage, 1),
                                                   HANDLER_ENTRY("EXPANDER", parseExpander, 1),
                                                   HANDLER_ENTRY("PAYLOAD", handlePayload, 1),
                                                   HANDLER_ENTRY("SCORER", handleScorer, 1),
                                                   HANDLER_ENTRY("SUMMARIZE", handleSummarize, 0),
                                                   HANDLER_ENTRY("HIGHLIGHT", handleHighlight, 0),
                                                   HANDLER_ENTRY("SORTBY", handleSortBy, 1),
                                                   HANDLER_ENTRY("INKEYS", handleInkeys, 1),
                                                   HANDLER_ENTRY("RETURN", handleReturn, 1)};

#define NUM_HANDLERS (sizeof(keywordHandlers_g) / sizeof(keywordHandlers_g[0]))

static int handleKeyword(RSSearchRequest *req, RedisSearchCtx *sctx, const char *keyword,
                         ArgsCursor *ac, QueryError *status) {

  const KeywordHandler *handler = NULL;
  for (size_t ii = 0; ii < NUM_HANDLERS; ++ii) {
    if (!strcasecmp(keyword, keywordHandlers_g[ii].keyword)) {
      handler = &keywordHandlers_g[ii];
      break;
    }
  }

  if (!handler) {
    // printf("Unknown keyword %s\n", RedisModule_StringPtrLen(argv[*offset], NULL));
    FMT_ERR(status, "Unknown keyword `%s`", keyword);
    return REDISMODULE_ERR;
  }

  if (AC_NumRemaining(ac) < handler->minArgs) {
    // printf("Insufficient args for %s\n", RedisModule_StringPtrLen(argv[*offset], NULL));
    FMT_ERR(errStr, "Insufficient arguments for `%s`", keyword);
    return REDISMODULE_ERR;
  }

  if (handler->parser(ac, req, &req->opts, sctx, status) != REDISMODULE_OK) {
    SET_ERR(status, handler->errStr);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              QueryError *status) {

  RSSearchRequest *req = calloc(1, sizeof(*req));
  *req = (RSSearchRequest){.opts = RS_DEFAULT_SEARCHOPTS, .payload = {.data = NULL, .len = 0}};

#define CUR_ARG_EQ(s) (!strcasecmp(curArg, s))
  const char *curArg = NULL;
  ArgsCursor ac;
  ArgsCursor_InitRString(&ac, argv, argc);
  if (AC_AdvanceBy(&ac, 3)) {
    goto opts_done;
  }

  while (AC_GetString(&ac, &curArg, NULL, 0) == 0) {
    if (CUR_ARG_EQ("NOCONTENT")) {
      req->opts.flags |= Search_NoContent;
    } else if (CUR_ARG_EQ("WITHSCORES")) {
      req->opts.flags |= Search_WithScores;
    } else if (CUR_ARG_EQ("WITHPAYLOADS")) {
      req->opts.flags |= Search_WithPayloads;
    } else if (CUR_ARG_EQ("WITHSORTKEYS")) {
      req->opts.flags |= Search_WithSortKeys;
    } else if (CUR_ARG_EQ("VERBATIM")) {
      req->opts.flags |= Search_Verbatim;
    } else if (CUR_ARG_EQ("INORDER")) {
      req->opts.flags |= Search_InOrder;
    } else if (CUR_ARG_EQ("NOSTOPWORDS")) {
      req->opts.flags |= Search_NoStopwrods;
    } else {
      if (handleKeyword(req, ctx, curArg, &ac, status) != REDISMODULE_OK) {
        goto err;
      }
    }
  }

opts_done:
  if (req->opts.fields.wantSummaries && !Index_SupportsHighlight(ctx->spec)) {
    SET_ERR(status, "HIGHLIGHT and SUMMARIZE not supported for this index");
    goto err;
  }

  if ((req->opts.flags & (Search_InOrder | Search_HasSlop)) == Search_InOrder) {
    // default when INORDER and no SLOP
    req->opts.slop = INT_MAX;
  }

  if (req->opts.fields.numFields > 0) {
    // Clear NOCONTENT (implicit or explicit) if returned fields are requested
    req->opts.flags &= ~Search_NoContent;
  }

  if (!req->opts.expander) {
    req->opts.expander = strdup(DEFAULT_EXPANDER_NAME);
  }

  FieldList_RestrictReturn(&req->opts.fields);

  req->rawQuery = (char *)RedisModule_StringPtrLen(argv[2], &req->qlen);
  req->rawQuery = strndup(req->rawQuery, req->qlen);
  return req;

err:
  RSSearchRequest_Free(req);
  return NULL;
}

static void ReturnedField_Free(ReturnedField *field) {
  free(field->highlightSettings.openTag);
  free(field->highlightSettings.closeTag);
  free(field->summarizeSettings.separator);
  free(field->name);
}

void FieldList_Free(FieldList *fields) {
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    ReturnedField_Free(fields->fields + ii);
  }
  ReturnedField_Free(&fields->defaultField);
  free(fields->fields);
}

ReturnedField *FieldList_GetCreateField(FieldList *fields, RedisModuleString *rname) {
  const char *name = RedisModule_StringPtrLen(rname, NULL);
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

void FieldList_RestrictReturn(FieldList *fields) {
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

void RSSearchRequest_Free(RSSearchRequest *req) {

  if (req->opts.indexName) free(req->opts.indexName);

  if (req->opts.expander) free(req->opts.expander);

  if (req->opts.scorer) free(req->opts.scorer);

  if (req->opts.language) free((char *)req->opts.language);

  if (req->rawQuery) free(req->rawQuery);

  if (req->geoFilter) {
    GeoFilter_Free(req->geoFilter);
  }

  if (req->idFilter) {
    IdFilter_Free(req->idFilter);
  }

  if (req->payload.data) {
    free(req->payload.data);
  }

  if (req->opts.sortBy) {

    RSSortingKey_Free(req->opts.sortBy);
  }

  if (req->numericFilters) {
    for (int i = 0; i < Vector_Size(req->numericFilters); i++) {
      NumericFilter *nf;
      Vector_Get(req->numericFilters, 0, &nf);
      if (nf) {
        NumericFilter_Free(nf);
      }
    }

    Vector_Free(req->numericFilters);
  }

  FieldList_Free(&req->opts.fields);

  free(req);
}

QueryParseCtx *SearchRequest_ParseQuery(RedisSearchCtx *sctx, RSSearchRequest *req,
                                        QueryError *status) {

  QueryParseCtx *q = NewQueryParseCtx(sctx, req->rawQuery, req->qlen, &req->opts);
  RedisModuleCtx *ctx = sctx->redisCtx;

  if (!Query_Parse(q, &status->detail)) {
    if (status->detail) {
      status->code = QUERY_ESYNTAX;
    }
    Query_Free(q);
    return NULL;
  }
  if (!(req->opts.flags & Search_Verbatim)) {
    Query_Expand(q, req->opts.expander);
  }

  if (req->geoFilter) {
    Query_SetGeoFilter(q, req->geoFilter);
    // Let the query tree handle the deletion
    req->geoFilter = NULL;
  }

  if (req->idFilter) {
    Query_SetIdFilter(q, req->idFilter);
  }
  // set numeric filters if possible
  if (req->numericFilters) {
    for (int i = 0; i < Vector_Size(req->numericFilters); i++) {
      NumericFilter *nf;
      Vector_Get(req->numericFilters, i, &nf);
      if (nf) {
        Query_SetNumericFilter(q, nf);
      }
    }

    Vector_Free(req->numericFilters);
    req->numericFilters = NULL;
  }
  return q;
}

QueryPlan *SearchRequest_BuildPlan(RedisSearchCtx *sctx, RSSearchRequest *req, QueryParseCtx *q,
                                   QueryError *status) {
  if (!q) return NULL;
  return Query_BuildPlan(sctx, q, &req->opts, Query_BuildProcessorChain, req, status);
}
