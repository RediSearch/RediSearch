#ifndef RS_SEARCH_REQUEST_H__
#define RS_SEARCH_REQUEST_H__

#include <stdlib.h>
#include "redisearch.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "id_filter.h"
#include "sortable.h"
#include "search_options.h"
#include "query_plan.h"
#include "query_error.h"

typedef struct {

  char *rawQuery;
  size_t qlen;

  RSSearchOptions opts;

  /* Numeric Filters */
  Vector *numericFilters;

  /* Geo Filter */
  GeoFilter *geoFilter;

  /* InKeys */
  IdFilter *idFilter;

  RSPayload payload;

} RSSearchRequest;

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              QueryError *status);

void RSSearchRequest_Free(RSSearchRequest *req);
QueryParseCtx *SearchRequest_ParseQuery(RedisSearchCtx *sctx, RSSearchRequest *req,
                                        QueryError *status);
QueryPlan *SearchRequest_BuildPlan(RedisSearchCtx *sctx, RSSearchRequest *req, QueryParseCtx *q,
                                   QueryError *error);

// Remove any fields not explicitly requested by `RETURN`, iff any explicit
// fields actually exist.
void FieldList_RestrictReturn(FieldList *fields);

#endif