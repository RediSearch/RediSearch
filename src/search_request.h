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

#define RS_DEFAULT_QUERY_FLAGS 0x00

// maximum results you can get in one query
#define SEARCH_REQUEST_RESULTS_MAX 1000000

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

  FieldList fields;

  RSPayload payload;

  RSSortingKey *sortBy;

} RSSearchRequest;

RSSearchRequest *ParseRequest(RedisSearchCtx *ctx, RedisModuleString **argv, int argc,
                              char **errStr);

void RSSearchRequest_Free(RSSearchRequest *req);

ReturnedField *FieldList_GetCreateField(FieldList *fields, RedisModuleString *rname);

// Remove any fields not explicitly requested by `RETURN`, iff any explicit
// fields actually exist.
void FieldList_RestrictReturn(FieldList *fields);

/* Process the request in the thread pool concurrently */
int RSSearchRequest_ProcessInThreadpool(RedisModuleCtx *ctx, RSSearchRequest *req);

/* Process the request in the main thread without context switching */
int RSSearchRequest_ProcessMainThread(RedisSearchCtx *sctx, RSSearchRequest *req);
#endif