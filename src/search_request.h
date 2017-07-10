#ifndef __RS_SEARCH_REQUEST_H__
#define __RS_SEARCH_REQUEST_H__

#include <stdlib.h>
#include "redisearch.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "id_filter.h"
#include "sortable.h"

typedef enum {
  Search_NoContent = 0x01,
  Search_Verbatim = 0x02,
  Search_NoStopwrods = 0x04,

  Search_WithScores = 0x08,
  Search_WithPayloads = 0x10,

  Search_InOrder = 0x20,

  Search_WithSortKeys = 0x40,

} RSSearchFlags;

#define RS_DEFAULT_QUERY_FLAGS 0x00

typedef struct {
  /* The index name - since we need to open the spec in a side thread */
  char *indexName;
  /* RS Context */
  RedisSearchCtx *sctx;
  RedisModuleBlockedClient *bc;

  char *rawQuery;
  size_t qlen;

  RSSearchFlags flags;

  /* Paging */
  size_t offset;
  size_t num;

  /* Numeric Filters */
  Vector *numericFilters;

  /* Geo Filter */
  GeoFilter *geoFilter;

  /* InKeys */
  IdFilter *idFilter;

  /* InFields */
  t_fieldMask fieldMask;

  int slop;

  char *language;

  char *expander;

  char *scorer;

  const char **retfields;
  size_t nretfields;

  RSPayload payload;

  RSSortingKey *sortBy;

} RSSearchRequest;

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr);

void RSSearchRequest_Free(RSSearchRequest *req);

int RSSearchRequest_Process(RedisModuleCtx *ctx, RSSearchRequest *req);

#endif