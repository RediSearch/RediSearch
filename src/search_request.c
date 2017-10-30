#include "search_request.h"
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
#include <sys/param.h>
#include <assert.h>

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
typedef int (*KeywordParser)(RedisModuleString **argv, int argc, size_t *offset,
                             RSSearchRequest *req, RedisSearchCtx *sctx, char **errStr);

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

#define KEYWORD_HANDLER(name)                                                               \
  static int name(RedisModuleString **argv, int argc, size_t *offset, RSSearchRequest *req, \
                  RedisSearchCtx *sctx, char **errStr)

KEYWORD_HANDLER(parseLimit) {
  ++*offset;

  long long tmpLimit, tmpOffset;
  if (RMUtil_ParseArgs(argv, argc, *offset, "ll", &tmpOffset, &tmpLimit) != REDISMODULE_OK) {
    // printf("Couldn't parse limit\n");
    return REDISMODULE_ERR;
  }

  if (tmpLimit <= 0 || tmpOffset < 0 || tmpOffset + tmpLimit > SEARCH_REQUEST_RESULTS_MAX) {
    *errStr = "LIMIT: Limit or offset too large";
    return REDISMODULE_ERR;
  }
  req->num = tmpLimit;
  req->offset = tmpOffset;
  *offset += 2;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseInfields) {
  size_t nargs;
  RedisModuleString **vargs = RMUtil_ParseVarArgs(argv, argc, *offset, "INFIELDS", &nargs);
  assert(vargs != NULL);

  if (nargs == RMUTIL_VARARGS_BADARG) {
    return REDISMODULE_ERR;
  }

  req->fieldMask = IndexSpec_ParseFieldMask(sctx->spec, vargs, nargs);
  RedisModule_Log(sctx->redisCtx, "debug", "Parsed field mask: 0x%x", req->fieldMask);
  // Add 2 (the keyword and number of args) to the args themselves
  *offset += 2 + nargs;
  return REDISMODULE_OK;
}

// {field} {min} {max}
KEYWORD_HANDLER(parseNumericFilter) {
  if (req->numericFilters == NULL) {
    req->numericFilters = NewVector(NumericFilter *, 2);
  }

  ++*offset;
  NumericFilter *flt = ParseNumericFilter(sctx, argv + *offset, 3);
  if (flt == NULL) {
    return REDISMODULE_ERR;
  }

  Vector_Push(req->numericFilters, flt);
  *offset += 3;
  // printf("Parsed filter: %f, %f\n", flt->min, flt->max);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseGeoFilter) {
  if (!req->geoFilter) {
    req->geoFilter = malloc(sizeof(*req->geoFilter));
  }

  ++*offset;
  int rv = GeoFilter_Parse(req->geoFilter, argv + *offset, 5);
  *offset += 5;
  return rv;
}

KEYWORD_HANDLER(parseSlop) {
  long long tmp;
  int rv = RedisModule_StringToLongLong(argv[++*offset], &tmp);
  req->slop = tmp;
  req->flags |= Search_HasSlop;
  ++*offset;
  return rv;
}

KEYWORD_HANDLER(parseLanguage) {
  // make sure we search for "language" only after the query
  const char *langTmp = RedisModule_StringPtrLen(argv[*offset], NULL);
  if (!IsSupportedLanguage(langTmp, strlen(langTmp))) {
    return REDISMODULE_ERR;
  }
  req->language = strdup(langTmp);
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(parseExpander) {
  req->expander = strdup(RedisModule_StringPtrLen(argv[++*offset], NULL));
  ++*offset;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handlePayload) {
  const char *payload = RedisModule_StringPtrLen(argv[(*offset)++], &req->payload.len);
  if (req->payload.len) {
    req->payload.data = malloc(req->payload.len);
    memcpy(req->payload.data, payload, req->payload.len);
  }
  ++*offset;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleScorer) {
  req->scorer = strdup(RedisModule_StringPtrLen(argv[++*offset], NULL));
  if (Extensions_GetScoringFunction(NULL, req->scorer) == NULL) {
    *errStr = "Invalid scorer name";
    return REDISMODULE_ERR;
  }
  ++*offset;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleSummarize) {
  return ParseSummarize(argv, argc, offset, &req->fields);
}

KEYWORD_HANDLER(handleHighlight) {
  return ParseHighlight(argv, argc, offset, &req->fields);
}

KEYWORD_HANDLER(handleSortBy) {
  RSSortingKey sortKey;
  int rc = RSSortingTable_ParseKey(sctx->spec->sortables, &sortKey, argv, argc, offset);
  if (rc == REDISMODULE_OK) {
    req->sortBy = malloc(sizeof(sortKey));
    *req->sortBy = sortKey;
  }
  return rc;
}

KEYWORD_HANDLER(handleInkeys) {
  size_t nargs;
  RedisModuleString **vargs = RMUtil_ParseVarArgs(argv, argc, 2, "INKEYS", &nargs);
  if (vargs == NULL || nargs == RMUTIL_VARARGS_BADARG) {
    return REDISMODULE_ERR;
  }
  req->idFilter = NewIdFilter(vargs, nargs, &sctx->spec->docs);
  *offset += 2 + nargs;
  return REDISMODULE_OK;
}

KEYWORD_HANDLER(handleReturn) {
  size_t nargs;
  RedisModuleString **vargs = RMUtil_ParseVarArgs(argv, argc, 2, "RETURN", &nargs);
  if (nargs == RMUTIL_VARARGS_BADARG) {
    return REDISMODULE_ERR;
  } else if (vargs == NULL || nargs == 0) {
    req->flags |= Search_NoContent;
  }

  for (size_t ii = 0; ii < nargs; ++ii) {
    ReturnedField *field = FieldList_GetCreateField(&req->fields, vargs[ii]);
    field->explicitReturn = 1;
  }
  req->fields.explicitReturn = 1;
  *offset += 2 + nargs;
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

static int handleKeyword(RSSearchRequest *req, RedisSearchCtx *sctx, RedisModuleString **argv,
                         int argc, size_t *offset, char **errStr) {

  const KeywordHandler *handler = NULL;

  for (size_t ii = 0; ii < NUM_HANDLERS; ++ii) {
    if (RMUtil_StringEqualsCaseC(argv[*offset], keywordHandlers_g[ii].keyword)) {
      handler = &keywordHandlers_g[ii];
      break;
    }
  }

  if (!handler) {
    // printf("Unknown keyword %s\n", RedisModule_StringPtrLen(argv[*offset], NULL));
    *errStr = "Unknown keyword";
    return REDISMODULE_ERR;
  }

  if (argc - (*offset + 1) < handler->minArgs) {
    // printf("Insufficient args for %s\n", RedisModule_StringPtrLen(argv[*offset], NULL));
    *errStr = (char *)handler->errStr;
    return REDISMODULE_ERR;
  }

  if (handler->parser(argv, argc, offset, req, sctx, errStr) != REDISMODULE_OK) {
    if (!*errStr) {
      *errStr = (char *)handler->errStr;
    }
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr) {

  *errStr = NULL;

  RSSearchRequest *req = calloc(1, sizeof(*req));
  *req = (RSSearchRequest){.sctx = NULL,
                           .indexName = strdup(RedisModule_StringPtrLen(argv[1], NULL)),
                           .bc = NULL,
                           .offset = 0,
                           .num = 10,
                           .payload = {.data = NULL, .len = 0},
                           .flags = RS_DEFAULT_QUERY_FLAGS,
                           .slop = -1,
                           .fieldMask = RS_FIELDMASK_ALL,
                           .sortBy = NULL};

#define CUR_ARG_EQ(s) RMUtil_StringEqualsCaseC(argv[offset], s)

#define CUR_ARG_EQ_INCR(s) (CUR_ARG_EQ(s) && ++offset)

#define ENSURE_ARG_REMAINS(argName)                 \
  if (offset == argc) {                             \
    *errStr = "Missing argument for `" argName "`"; \
    goto err;                                       \
  }

  size_t offset = 3;

  while (offset < argc) {
    if (CUR_ARG_EQ_INCR("NOCONTENT")) {
      req->flags |= Search_NoContent;
    } else if (CUR_ARG_EQ_INCR("WITHSCORES")) {
      req->flags |= Search_WithScores;
    } else if (CUR_ARG_EQ_INCR("WITHPAYLOADS")) {
      req->flags |= Search_WithPayloads;
    } else if (CUR_ARG_EQ_INCR("WITHSORTKEYS")) {
      req->flags |= Search_WithSortKeys;
    } else if (CUR_ARG_EQ_INCR("VERBATIM")) {
      req->flags |= Search_Verbatim;
    } else if (CUR_ARG_EQ_INCR("INORDER")) {
      req->flags |= Search_InOrder;
    } else {
      if (handleKeyword(req, ctx, argv, argc, &offset, errStr) != REDISMODULE_OK) {
        goto err;
      }
    }
  }

  if ((req->flags & (Search_InOrder | Search_HasSlop)) == Search_InOrder) {
    // default when INORDER and no SLOP
    req->slop = INT_MAX;
  }

  if (req->fields.numFields > 0) {
    // Clear NOCONTENT (implicit or explicit) if returned fields are requested
    req->flags &= ~Search_NoContent;
  }

  if (!req->expander) {
    req->expander = strdup(DEFAULT_EXPANDER_NAME);
  }

  FieldList_RestrictReturn(&req->fields);

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

static void FieldList_Free(FieldList *fields) {
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    ReturnedField_Free(fields->fields + ii);
  }
  ReturnedField_Free(&fields->defaultField);
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

  if (req->indexName) free(req->indexName);

  if (req->expander) free(req->expander);

  if (req->scorer) free(req->scorer);

  if (req->language) free(req->language);

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

  if (req->sortBy) {

    RSSortingKey_Free(req->sortBy);
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

  FieldList_Free(&req->fields);

  if (req->sctx) {
    SearchCtx_Free(req->sctx);
  }

  free(req);
}

int runQueryGeneric(RSSearchRequest *req, int concurrentMode) {

  QueryParseCtx *q = NewQueryParseCtx(req);
  RedisModuleCtx *ctx = req->sctx->redisCtx;

  char *err;
  if (!Query_Parse(q, &err)) {

    if (err) {
      RedisModule_Log(ctx, "debug", "Error parsing query: %s", err);
      RedisModule_ReplyWithError(ctx, err);
      free(err);
    } else {
      /* Simulate an empty response - this means an empty query */
      RedisModule_ReplyWithArray(ctx, 1);
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
    Query_Free(q);
    return REDISMODULE_ERR;
  }
  if (!(req->flags & Search_Verbatim)) {
    Query_Expand(q, req->expander);
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

  QueryPlan *plan = Query_BuildPlan(q, req, concurrentMode);
  // Execute the query
  // const char *err;
  int rc = QueryPlan_Execute(plan, (const char **)&err);
  if (rc == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
  }
  QueryPlan_Free(plan);
  Query_Free(q);

  return rc;
}

// process the query in the thread pool - thread pool callback
void threadProcessQuery(void *p) {
  RSSearchRequest *req = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(req->bc);
  RedisModule_AutoMemory(ctx);

  RedisModule_ThreadSafeContextLock(ctx);
  req->sctx =
      NewSearchCtx(ctx, RedisModule_CreateString(ctx, req->indexName, strlen(req->indexName)));

  if (!req->sctx) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
  } else {
    runQueryGeneric(req, 1);
  }

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_UnblockClient(req->bc, NULL);
  RSSearchRequest_Free(req);
  RedisModule_FreeThreadSafeContext(ctx);

  return;
  //  return REDISMODULE_OK;
}

int RSSearchRequest_ProcessInThreadpool(RedisModuleCtx *ctx, RSSearchRequest *req) {
  req->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  ConcurrentSearch_ThreadPoolRun(threadProcessQuery, req, CONCURRENT_POOL_SEARCH);
  return REDISMODULE_OK;
}

int RSSearchRequest_ProcessMainThread(RedisSearchCtx *sctx, RSSearchRequest *req) {
  req->sctx = sctx;
  req->bc = NULL;

  int rc = runQueryGeneric(req, 0);
  RSSearchRequest_Free(req);
  return rc;
}
