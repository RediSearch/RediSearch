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

#define BAD_LENGTH_ARGS ((size_t)-1)
/**
 * Gets a variable list of arguments to a given keyword.
 * The number of variable arguments are found in `outlen`.
 * If offset is specified, the first <offset> arguments will be ignored when
 * checking for the presence of the keyword and subsequent arguments.
 *
 * If the return value is NULL then the keyword is not present. If
 * the value of outlen is `BAD_LENGTH_ARGS` then the keyword was found, but
 * there was a problem with how the numbers are formatted.
 *
 */
static RedisModuleString **getLengthArgs(const char *keyword, size_t *outlen,
                                         RedisModuleString **argv, int argc, size_t offset) {
  if (offset > argc) {
    return NULL;
  }

  argv += offset;
  argc -= offset;

  int ix = RMUtil_ArgIndex(keyword, argv, argc);
  if (ix < 0) {
    return NULL;
  } else if (ix >= argc - 1) {
    *outlen = BAD_LENGTH_ARGS;
    return argv;
  }

  argv += (ix + 1);
  argc -= (ix + 1);

  long long n = 0;
  RMUtil_ParseArgs(argv, argc, 0, "l", &n);
  if (n > argc - 1 || n < 0) {
    *outlen = BAD_LENGTH_ARGS;
    return argv;
  }

  *outlen = n;
  return argv + 1;
}

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

  // Parse LIMIT argument
  long long first = 0, limit = 10;
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &req->offset, &req->num);
  if (req->num <= 0) {
    *errStr = "Wrog Arity";
    goto err;
  }

  RedisModuleString **vargs;
  size_t nargs;

  // if INFIELDS exists, parse the field mask
  req->fieldMask = RS_FIELDMASK_ALL;
  if ((vargs = getLengthArgs("INFIELDS", &nargs, argv, argc, 3))) {
    if (nargs == BAD_LENGTH_ARGS) {
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
  if ((vargs = getLengthArgs("INKEYS", &nargs, argv, argc, 2))) {
    if (nargs == BAD_LENGTH_ARGS) {
      *errStr = "Bad argument for `INKEYS`";
      goto err;
    }
    req->idFilter = NewIdFilter(vargs, nargs, &ctx->spec->docs);
  }

  // parse RETURN argument
  if ((vargs = getLengthArgs("RETURN", &nargs, argv, argc, 2))) {
    if (!nargs) {
      req->flags |= Search_NoContent;
    } else {
      req->retfields = malloc(sizeof(*req->retfields) * nargs);
      req->nretfields = nargs;
      for (size_t ii = 0; ii < nargs; ++ii) {
        req->retfields[ii] = strdup(RedisModule_StringPtrLen(vargs[ii], NULL));
      }
    }
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

  if (req->retfields) {
    for (size_t ii = 0; ii < req->nretfields; ++ii) {
      free((void *)req->retfields[ii]);
    }
    free(req->retfields);
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

  QueryResult_Serialize(r, req->sctx, req);
  QueryResult_Free(r);
  Query_Free(q);

end:
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_UnblockClient(req->bc, NULL);
  RSSearchRequest_Free(req);
  RedisModule_FreeThreadSafeContext(ctx);

  return;
  //  return REDISMODULE_OK;
}

int RSSearchRequest_Process(RedisModuleCtx *ctx, RSSearchRequest *req) {
  req->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  ConcurrentSearch_ThreadPoolRun(threadProcessQuery, req);
  return REDISMODULE_OK;
}
