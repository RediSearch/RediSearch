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
#include <err.h>

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr) {

  RSSearchRequest *req = calloc(1, sizeof(*req));
  *req = (RSSearchRequest){
      .opts = RS_DEFAULT_SEARCHOPTS,
  };
  req->opts.indexName = strdup(RedisModule_StringPtrLen(argv[1], NULL));

  // Detect "NOCONTENT"
  if (RMUtil_ArgExists("NOCONTENT", argv, argc, 3)) req->opts.flags |= Search_NoContent;

  // parse WISTHSCORES
  if (RMUtil_ArgExists("WITHSCORES", argv, argc, 3)) req->opts.flags |= Search_WithScores;

  // parse WITHPAYLOADS
  if (RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3)) req->opts.flags |= Search_WithPayloads;

  // parse WITHSORTKEYS
  if (RMUtil_ArgExists("WITHSORTKEYS", argv, argc, 3)) req->opts.flags |= Search_WithSortKeys;

  // Parse VERBATIM and LANGUAGE arguments
  if (RMUtil_ArgExists("VERBATIM", argv, argc, 3)) req->opts.flags |= Search_Verbatim;

  // Parse NOSTOPWORDS argument
  if (RMUtil_ArgExists("NOSTOPWORDS", argv, argc, 3)) req->opts.flags |= Search_NoStopwrods;

  if (RMUtil_ArgExists("INORDER", argv, argc, 3)) {
    req->opts.flags |= Search_InOrder;
    // the slop will be parsed later, this is just the default when INORDER and no SLOP
    req->opts.slop = __INT_MAX__;
  }

  int sumIdx;
  if ((sumIdx = RMUtil_ArgExists("SUMMARIZE", argv, argc, 3)) > 0) {
    size_t tmpOffset = sumIdx;
    if (ParseSummarize(argv, argc, &tmpOffset, &req->opts.fields) != REDISMODULE_OK) {
      SET_ERR(errStr, "Couldn't parse `SUMMARIZE`");
      goto err;
    }
  }

  if ((sumIdx = RMUtil_ArgExists("HIGHLIGHT", argv, argc, 3)) > 0) {
    // Parse the highlighter spec
    size_t tmpOffset = sumIdx;
    if (ParseHighlight(argv, argc, &tmpOffset, &req->opts.fields) != REDISMODULE_OK) {
      SET_ERR(errStr, "Couldn't parse `HIGHLIGHT`");
      goto err;
    }
  }

  // Parse LIMIT argument
  long long offset = 0, limit = 10;
  if (RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &offset, &limit) == REDISMODULE_OK) {
    if (offset < 0 || limit <= 0 || (offset + limit > SEARCH_REQUEST_RESULTS_MAX)) {
      SET_ERR(errStr, "Invalid LIMIT parameters");
      goto err;
    }
  }
  req->opts.offset = offset;
  req->opts.num = limit;

  RedisModuleString **vargs;
  size_t nargs;

  // if INFIELDS exists, parse the field mask
  req->opts.fieldMask = RS_FIELDMASK_ALL;
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 3, "INFIELDS", &nargs))) {
    if (nargs == RMUTIL_VARARGS_BADARG) {
      SET_ERR(errStr, "Bad argument for `INFIELDS`");
      goto err;
    }
    req->opts.fieldMask = IndexSpec_ParseFieldMask(ctx->spec, vargs, nargs);
    RedisModule_Log(ctx->redisCtx, "debug", "Parsed field mask: 0x%x", req->opts.fieldMask);
  }

  // Parse numeric filter. currently only one supported
  int filterIdx = RMUtil_ArgExists("FILTER", argv, argc, 3);
  if (filterIdx > 0) {
    req->numericFilters = ParseMultipleFilters(ctx, &argv[filterIdx], argc - filterIdx);
    if (req->numericFilters == NULL) {
      SET_ERR(errStr, "Invalid numeric filter");
      goto err;
    }
  }

  // parse geo filter if present
  int gfIdx = RMUtil_ArgExists("GEOFILTER", argv, argc, 3);
  if (gfIdx > 0 && gfIdx + 6 <= argc) {
    req->geoFilter = malloc(sizeof(GeoFilter));
    if (GeoFilter_Parse(req->geoFilter, &argv[gfIdx + 1], 5) == REDISMODULE_ERR) {
      SET_ERR(errStr, "Invalid geo filter");
      goto err;
    }
  }

  RMUtil_ParseArgsAfter("SLOP", argv, argc, "l", &req->opts.slop);

  // make sure we search for "language" only after the query
  if (argc > 3) {
    RMUtil_ParseArgsAfter("LANGUAGE", &argv[3], argc - 3, "c", &req->opts.language);
    if (req->opts.language &&
        !IsSupportedLanguage(req->opts.language, strlen(req->opts.language))) {
      SET_ERR(errStr, "Unsupported Stemmer Language");
      req->opts.language = NULL;
      goto err;
    }
    if (req->opts.language) req->opts.language = strdup(req->opts.language);
  }

  // parse the optional expander argument
  if (argc > 3) {
    RMUtil_ParseArgsAfter("EXPANDER", &argv[2], argc - 2, "c", &req->opts.expander);
  }
  if (!req->opts.expander) {
    req->opts.expander = DEFAULT_EXPANDER_NAME;
  }
  req->opts.expander = strdup(req->opts.expander);

  // IF a payload exists, init it
  req->payload = (RSPayload){.data = NULL, .len = 0};

  if (argc > 3) {
    RedisModuleString *ps = NULL;
    RMUtil_ParseArgsAfter("PAYLOAD", &argv[2], argc - 2, "s", &ps);
    if (ps) {

      const char *data = RedisModule_StringPtrLen(ps, &req->payload.len);
      req->payload.data = malloc(req->payload.len);
      memcpy(req->payload.data, data, req->payload.len);
      req->opts.payload = &req->payload;
    }
  }

  // parse SCORER argument

  RMUtil_ParseArgsAfter("SCORER", &argv[3], argc - 3, "c", &req->opts.scorer);
  if (req->opts.scorer) {
    req->opts.scorer = strdup(req->opts.scorer);
    if (Extensions_GetScoringFunction(NULL, req->opts.scorer) == NULL) {
      SET_ERR(errStr, "Invalid scorer name");
      goto err;
    }
  }

  // Parse SORTBY argument
  RSSortingKey sortKey;
  if (ctx->spec->sortables != NULL &&
      RSSortingTable_ParseKey(ctx->spec->sortables, &sortKey, &argv[3], argc - 3)) {
    req->opts.sortBy = malloc(sizeof(RSSortingKey));
    *req->opts.sortBy = sortKey;
  }

  // parse the id filter arguments
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 2, "INKEYS", &nargs))) {
    if (nargs == RMUTIL_VARARGS_BADARG) {
      SET_ERR(errStr, "Bad argument for `INKEYS`");
      goto err;
    }
    req->idFilter = NewIdFilter(vargs, nargs, &ctx->spec->docs);
  }

  // parse RETURN argument
  if ((vargs = RMUtil_ParseVarArgs(argv, argc, 2, "RETURN", &nargs))) {
    if (nargs == RMUTIL_VARARGS_BADARG) {
      SET_ERR(errStr, "Bad argument for `RETURN`");
      goto err;
    }
    if (!nargs) {
      req->opts.flags |= Search_NoContent;
    } else {
      req->opts.fields.explicitReturn = 1;
      for (size_t ii = 0; ii < nargs; ++ii) {
        ReturnedField *rf = FieldList_GetCreateField(&req->opts.fields, vargs[ii]);
        rf->explicitReturn = 1;
      }
    }
  }

  if (req->opts.fields.wantSummaries && !Index_SupportsHighlight(ctx->spec)) {
    SET_ERR(errStr, "HIGHLIGHT and SUMMARIZE not supported for this index");
    goto err;
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

QueryParseCtx *SearchRequest_ParseQuery(RedisSearchCtx *sctx, RSSearchRequest *req, char **err) {

  QueryParseCtx *q = NewQueryParseCtx(sctx, req->rawQuery, req->qlen, &req->opts);
  RedisModuleCtx *ctx = sctx->redisCtx;

  if (!Query_Parse(q, err)) {
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
                                   char **err) {
  if (!q) return NULL;
  return Query_BuildPlan(sctx, q, &req->opts, Query_BuildProcessorChain, req, err);
}
