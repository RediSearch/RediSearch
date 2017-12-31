#ifndef __RS_SEARCH_REQUEST_H__
#define __RS_SEARCH_REQUEST_H__

#include <stdlib.h>
#include "redisearch.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "id_filter.h"
#include "sortable.h"
#include "search_options.h"
#include "query_plan.h"

typedef enum {
  // No summaries
  SummarizeMode_None = 0x00,
  SummarizeMode_Highlight = 0x01,
  SummarizeMode_Synopsis = 0x02
} SummarizeMode;

#define SUMMARIZE_MODE_DEFAULT SummarizeMode_Synopsis
#define SUMMARIZE_FRAGSIZE_DEFAULT 20
#define SUMMARIZE_FRAGCOUNT_DEFAULT 3
#define SUMMARIZE_DEFAULT_OPEN_TAG "<b>"
#define SUMMARIZE_DEFAULT_CLOSE_TAG "</b>"
#define SUMMARIZE_DEFAULT_SEPARATOR "... "

typedef struct {
  uint32_t contextLen;
  uint16_t numFrags;
  char *separator;
} SummarizeSettings;

typedef struct {
  char *openTag;
  char *closeTag;
} HighlightSettings;

typedef struct {
  char *name;
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  // Whether this field was explicitly requested by `RETURN`
  int explicitReturn;
} ReturnedField;

typedef struct {
  ReturnedField defaultField;

  // List of individual field specifications
  ReturnedField *fields;
  size_t numFields;
  uint16_t wantSummaries;
  // Whether this list contains fields explicitly selected by `RETURN`
  uint16_t explicitReturn;
} FieldList;

typedef struct {

  /* RS Context */
  RedisSearchCtx *sctx;

  char *rawQuery;
  size_t qlen;

  RSSearchOptions opts;

  /* Numeric Filters */
  Vector *numericFilters;

  /* Geo Filter */
  GeoFilter *geoFilter;

  /* InKeys */
  IdFilter *idFilter;

  FieldList fields;

  RSPayload payload;

} RSSearchRequest;

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr);

void RSSearchRequest_Free(RSSearchRequest *req);
QueryPlan *SearchRequest_BuildPlan(RSSearchRequest *req, char **err);
ReturnedField *FieldList_GetCreateField(FieldList *fields, RedisModuleString *rname);

// Remove any fields not explicitly requested by `RETURN`, iff any explicit
// fields actually exist.
void FieldList_RestrictReturn(FieldList *fields);

/* Process the request in the thread pool concurrently */
int RSSearchRequest_ProcessInThreadpool(RedisModuleCtx *ctx, RSSearchRequest *req);
int RSSearchRequest_ProcessAggregateRequet(RSSearchRequest *req, RedisModuleString **argv,
                                           int argc);
/* Process the request in the main thread without context switching */
int RSSearchRequest_ProcessMainThread(RedisSearchCtx *sctx, RSSearchRequest *req);
#endif