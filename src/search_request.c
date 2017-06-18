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
#include <sys/param.h>

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr) {

  RSSearchRequest *req = calloc(1, sizeof(*req));
  *req = (RSSearchRequest){
      .sctx = NULL,
      .indexName = strdup(RedisModule_StringPtrLen(argv[1], NULL)),
      .bc = NULL,
      .offset = 0,
      .num = 10,
      .flags = RS_DEFAULT_QUERY_FLAGS,
      .slop = -1,
      .fieldMask = RS_FIELDMASK_ALL,
  };

  // Detect "NOCONTENT"
  if (RMUtil_ArgExists("NOCONTENT", argv, argc, 3)) req->flags |= Search_NoContent;

  // parse WISTHSCORES
  if (RMUtil_ArgExists("WITHSCORES", argv, argc, 3)) req->flags |= Search_WithScores;

  // parse WITHPAYLOADS
  if (RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3)) req->flags |= Search_WithPayloads;

  // Parse VERBATIM and LANGUAGE arguments
  if (RMUtil_ArgExists("VERBATIM", argv, argc, 3)) req->flags |= Search_Verbatim;

  // Parse NOSTOPWORDS argument
  if (RMUtil_ArgExists("NOSTOPWORDS", argv, argc, 3)) req->flags |= Search_NoStopwrods;

  if (RMUtil_ArgExists("INORDER", argv, argc, 3)) {
    req->flags |= Search_InOrder;
    // the slop will be parsed later, this is just the default when INORDER and no SLOP
    req->slop = __INT_MAX__;
  }

  // Parse LIMIT argument
  long long first = 0, limit = 10;
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &req->offset, &req->num);
  if (req->num <= 0) {
    *errStr = "Wrog Arity";
    goto err;
  }

  // if INFIELDS exists, parse the field mask
  int inFieldsIdx = RMUtil_ArgIndex("INFIELDS", argv, argc);
  long long numFields = 0;
  req->fieldMask = RS_FIELDMASK_ALL;
  if (inFieldsIdx >= 3) {
    RMUtil_ParseArgs(argv, argc, inFieldsIdx + 1, "l", &numFields);
    if (numFields > 0 && inFieldsIdx + 1 + numFields < argc) {
      req->fieldMask = IndexSpec_ParseFieldMask(ctx->spec, &argv[inFieldsIdx + 2], numFields);
    }
    RedisModule_Log(ctx->redisCtx, "warning", "Parsed field mask: 0x%x\n", req->fieldMask);
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
  if (gfIdx > 0 && filterIdx + 6 <= argc) {
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
  if (RSSortingTable_ParseKey(ctx->spec->sortables, &sortKey, &argv[3], argc - 3)) {
    req->sortBy = malloc(sizeof(RSSortingKey));
    *req->sortBy = sortKey;
  }

  // parse the id filter arguments
  long long numFilteredIds = 0;
  IdFilter idf = (IdFilter){.ids = NULL, .keys = NULL};
  RMUtil_ParseArgsAfter("INKEYS", &argv[2], argc - 2, "l", &numFilteredIds);
  if (numFilteredIds > 0 && numFilteredIds < argc - 3) {

    RedisModule_Log(ctx->redisCtx, "debug", "Filtering %d keys", numFilteredIds);
    int pos = RMUtil_ArgIndex("INKEYS", argv, argc);
    req->idFilter =
        NewIdFilter(&argv[pos + 2], MIN(argc - pos - 2, numFilteredIds), &ctx->spec->docs);
  }

  req->rawQuery = (char *)RedisModule_StringPtrLen(argv[2], &req->qlen);
  req->rawQuery = strndup(req->rawQuery, req->qlen);
  return req;

err:
  RSSearchRequest_Free(req);
  return NULL;
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

  if (req->sctx) {
    SearchCtx_Free(req->sctx);
  }

  free(req);
}

void threadProcessQuery(void *p) {
  RSSearchRequest *req = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(req->bc);
  RedisModule_AutoMemory(ctx);

  RedisModule_ThreadSafeContextLock(ctx);

  req->sctx =
      NewSearchCtx(ctx, RedisModule_CreateString(ctx, req->indexName, strlen(req->indexName)));
  if (!req->sctx) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    goto end;
  }

  Query *q = NewQueryFromRequest(req);
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
    goto end;
  }

  Query_Expand(q);

  if (req->geoFilter) {
    Query_SetGeoFilter(q, req->geoFilter);
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

  // Execute the query
  QueryResult *r = Query_Execute(q);
  if (r == NULL) {
    RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
    goto end;
  }

  QueryResult_Serialize(r, req->sctx, req->flags);
  QueryResult_Free(r);
  Query_Free(q);

end:
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_UnblockClient(req->bc, NULL);
  RedisModule_FreeThreadSafeContext(ctx);
  RSSearchRequest_Free(req);

  return;
  //  return REDISMODULE_OK;
}

int RSSearchRequest_Process(RedisModuleCtx *ctx, RSSearchRequest *req) {
  req->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  ConcurrentSearch_ThreadPoolRun(threadProcessQuery, req);
  return REDISMODULE_OK;
}
