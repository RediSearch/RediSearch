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

  // Use best fragments, stringing together by elipsis
  SummarizeMode_Synopsis,

  // Return a list of fragments, ordered by relevance
  SummarizeMode_ByRelevance,

  // Return a list of fragments, ordered by their appearance in the document
  SummarizeMode_ByOrder,

  // Return a list of top fragments, ordered by appearance
  SummarizeMode_ByRelOrder,

  // Return the entire field highlighted
  SummarizeMode_WholeField
} SummarizeMode;

#define SUMMARIZE_MODE_DEFAULT SummarizeMode_Synopsis
#define SUMMARIZE_FRAGSIZE_DEFAULT 20
#define SUMMARIZE_FRAGCOUNT_DEFAULT 3

typedef struct {
  char *openTag;
  char *closeTag;
  uint32_t contextLen;
  uint16_t nameIndex;
  uint16_t numFrags;
  SummarizeMode mode;
} ReturnedField;

typedef struct {
  char *openTag;
  char *closeTag;

  ReturnedField *fields;
  size_t numFields;

  char **rawFields;
  uint32_t numRawFields;
  uint32_t wantSummaries;
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

ReturnedField *FieldList_AddField(FieldList *fields, const char *name);
ReturnedField *FieldList_AddFieldR(FieldList *fields, RedisModuleString *name);

/* Process the request in the thread pool concurrently */
int RSSearchRequest_ProcessInThreadpool(RedisModuleCtx *ctx, RSSearchRequest *req);

/* Process the request in the main thread without context switching */
int RSSearchRequest_ProcessMainThread(RedisSearchCtx *sctx, RSSearchRequest *req);
#endif