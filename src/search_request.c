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

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr) {

  RSSearchRequest *req = calloc(1, sizeof(*req));
  *req = (RSSearchRequest){.sctx = NULL,
                           .indexName = strdup(RedisModule_StringPtrLen(argv[1], NULL)),
                           .bc = NULL,
                           .offset = 0,
                           .num = 10,
                           .flags = RS_DEFAULT_QUERY_FLAGS,
                           .slop = -1,
                           .fieldMask = RS_FIELDMASK_ALL,
                           .sortBy = NULL};

  // Detect "NOCONTENT"
  if (RMUtil_ArgExists("NOCONTENT", argv, argc, 3)) req->flags |= Search_NoContent;

  // parse WISTHSCORES
  if (RMUtil_ArgExists("WITHSCORES", argv, argc, 3)) req->flags |= Search_WithScores;

  // parse WITHPAYLOADS
  if (RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3)) req->flags |= Search_WithPayloads;

  // parse WITHSORTKEYS
  if (RMUtil_ArgExists("WITHSORTKEYS", argv, argc, 3)) req->flags |= Search_WithSortKeys;

  // Parse VERBATIM and LANGUAGE arguments
  if (RMUtil_ArgExists("VERBATIM", argv, argc, 3)) req->flags |= Search_Verbatim;

  // Parse NOSTOPWORDS argument
  if (RMUtil_ArgExists("NOSTOPWORDS", argv, argc, 3)) req->flags |= Search_NoStopwrods;

  if (RMUtil_ArgExists("INORDER", argv, argc, 3)) {
    req->flags |= Search_InOrder;
    // the slop will be parsed later, this is just the default when INORDER and no SLOP
    req->slop = __INT_MAX__;
  }

  int sumIdx;
  if ((sumIdx = RMUtil_ArgExists("SUMMARIZE", argv, argc, 3)) > 0) {
    size_t tmpOffset = sumIdx;
    if (ParseSummarize(argv, argc, &tmpOffset, &req->fields) != REDISMODULE_OK) {
      *errStr = "Couldn't parse `SUMMARIZE`";
      goto err;
    }
  }

  if ((sumIdx = RMUtil_ArgExists("HIGHLIGHT", argv, argc, 3)) > 0) {
    // Parse the highlighter spec
    size_t tmpOffset = sumIdx;
    if (ParseHighlight(argv, argc, &tmpOffset, &req->fields) != REDISMODULE_OK) {
      *errStr = "Couldn't parse `HIGHLIGHT`";
      goto err;
    }
  }

  // Parse LIMIT argument
  long long offset = 0, limit = 10;
  if (RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &offset, &limit) == REDISMODULE_OK) {
    if (offset < 0 || limit <= 0 || (offset + limit > SEARCH_REQUEST_RESULTS_MAX)) {
      *errStr = "Invalid LIMIT parameters";
      goto err;
    }
  }
  req->offset = offset;
  req->num = limit;

  RedisModuleString **vargs;
  size_t nargs;

  // if INFIELDS exists, parse the field mask
  req->fieldMask = RS_FIELDMASK_ALL;
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 3, "INFIELDS", &nargs))) {
    if (nargs == RMUTIL_VARARGS_BADARG) {
      *errStr = "Bad argument for `INFIELDS`";
      goto err;
    }
    req->fieldMask = IndexSpec_ParseFieldMask(ctx->spec, vargs, nargs);
    RedisModule_Log(ctx->redisCtx, "debug", "Parsed field mask: 0x%x", req->fieldMask);
  }

  // Parse numeric filter. currently only one supported
  int filterIdx = RMUtil_ArgExists("FILTER", argv, argc, 3);
  if (filterIdx > 0) {
    req->numericFilters = ParseMultipleFilters(ctx, &argv[filterIdx], argc - filterIdx);
    if (req->numericFilters == NULL) {
      *errStr = "Invalid numeric filter";
      goto err;
    }
  }

  // parse geo filter if present
  int gfIdx = RMUtil_ArgExists("GEOFILTER", argv, argc, 3);
  if (gfIdx > 0 && gfIdx + 6 <= argc) {
    req->geoFilter = malloc(sizeof(GeoFilter));
    if (GeoFilter_Parse(req->geoFilter, &argv[gfIdx + 1], 5) == REDISMODULE_ERR) {
      *errStr = "Invalid geo filter";
      goto err;
    }
  }

  RMUtil_ParseArgsAfter("SLOP", argv, argc, "l", &req->slop);

  // make sure we search for "language" only after the query
  if (argc > 3) {
    RMUtil_ParseArgsAfter("LANGUAGE", &argv[3], argc - 3, "c", &req->language);
    if (req->language && !IsSupportedLanguage(req->language, strlen(req->language))) {
      *errStr = "Unsupported Stemmer Language";
      req->language = NULL;
      goto err;
    }
    if (req->language) req->language = strdup(req->language);
  }

  // parse the optional expander argument
  if (argc > 3) {
    RMUtil_ParseArgsAfter("EXPANDER", &argv[2], argc - 2, "c", &req->expander);
  }
  if (!req->expander) {
    req->expander = DEFAULT_EXPANDER_NAME;
  }
  req->expander = strdup(req->expander);

  // IF a payload exists, init it
  req->payload = (RSPayload){.data = NULL, .len = 0};

  if (argc > 3) {
    RedisModuleString *ps = NULL;
    RMUtil_ParseArgsAfter("PAYLOAD", &argv[2], argc - 2, "s", &ps);
    if (ps) {

      const char *data = RedisModule_StringPtrLen(ps, &req->payload.len);
      req->payload.data = malloc(req->payload.len);
      memcpy(req->payload.data, data, req->payload.len);
    }
  }

  // parse SCORER argument

  RMUtil_ParseArgsAfter("SCORER", &argv[3], argc - 3, "c", &req->scorer);
  if (req->scorer) {
    req->scorer = strdup(req->scorer);
    if (Extensions_GetScoringFunction(NULL, req->scorer) == NULL) {
      *errStr = "Invalid scorer name";
      goto err;
    }
  }

  // Parse SORTBY argument
  RSSortingKey sortKey;
  if (ctx->spec->sortables != NULL &&
      RSSortingTable_ParseKey(ctx->spec->sortables, &sortKey, &argv[3], argc - 3)) {
    req->sortBy = malloc(sizeof(RSSortingKey));
    *req->sortBy = sortKey;
  }

  // parse the id filter arguments
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 2, "INKEYS", &nargs))) {
    if (nargs == RMUTIL_VARARGS_BADARG) {
      *errStr = "Bad argument for `INKEYS`";
      goto err;
    }
    req->idFilter = NewIdFilter(vargs, nargs, &ctx->spec->docs);
  }

  // parse RETURN argument
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 2, "RETURN", &nargs))) {
    if (!nargs) {
      req->flags |= Search_NoContent;
    } else {
      req->fields.explicitReturn = 1;
      for (size_t ii = 0; ii < nargs; ++ii) {
        ReturnedField *rf = FieldList_GetCreateField(&req->fields, vargs[ii]);
        rf->explicitReturn = 1;
      }
    }
  }

  if (req->fields.wantSummaries && !Index_SupportsHighlight(ctx->spec)) {
    *errStr = "HIGHLIGHT and SUMMARIZE not supported for this index";
    goto err;
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
