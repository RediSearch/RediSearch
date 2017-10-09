#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>

#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "geo_index.h"
#include "query_node.h"
#include "query_parser/tokenizer.h"
#include "redis_index.h"
#include "redismodule.h"
#include "spec.h"
#include "id_filter.h"
#include "redisearch.h"
#include "rmutil/sds.h"
#include "search_request.h"
#include "concurrent_ctx.h"
#include "value.h"

typedef struct {
  t_docId docId;

  // not all results have score - TBD
  double score;

  RSSortingVector *sv;

  RSDocumentMetadata *md;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  RSIndexResult *indexResult;

  // dynamic fields
  RSFieldMap *fields;
} SearchResult;

#define RS_RESULT_OK 0
#define RS_RESULT_QUEUED 1
#define RS_RESULT_EOF 2

struct resultProcessor;

typedef enum {
  QueryState_OK,
  QueryState_Aborted,
  QueryState_Error,
} QueryState;

typedef struct {
  ConcurrentSearchCtx *conc;
  double minScore;
  uint32_t totalResults;
  char *errorString;
  QueryState state;
} SearchResultCtx;

typedef struct {
  void *privdata;
  struct resultProcessor *upstream;
  SearchResultCtx *resCtx;
} ResultProcessorCtx;

typedef struct resultProcessor {
  // the context should contain a pointer to the upstream step
  // like the index iterator does
  ResultProcessorCtx ctx;

  // Next is called by the downstream processor, and should return either:
  // * RS_RESULT_OK -> means we put something in the result pointer and it can be processed
  // * RS_RESULT_QUEUED -> no result yet, we're waiting for more results upstream. Caller should
  //   return QUEUED as well
  // * RS_RESULT_EOF -> finished, nothing more from this processor
  int (*Next)(ResultProcessorCtx *ctx, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct resultProcessor *p);
} ResultProcessor;

ResultProcessor *NewResultProcessor(ResultProcessor *upstream, void *privdata);

static inline int ResultProcessor_Next(ResultProcessor *rp, SearchResult *res, int allowSwitching);

/* Helper function - get the total from a processor, and if the Total callback is NULL, climb up the
chain until we find a processor with a Total callback. This allows processors to avoid implementing
it if they have no calculations to add to Total (such as deeted/ignored results) */
static inline size_t ResultProcessor_Total(ResultProcessor *rp) {
  return rp->ctx.resCtx->totalResults;
}

static void ResultProcessor_Free(ResultProcessor *rp) {
  ResultProcessor *upstream = rp->ctx.upstream;
  if (rp->Free) {
    rp->Free(rp);
  } else {
    // For processors that did not bother to define a special Free - we just call free()
    free(rp);
  }
  // continue to the upstream processor
  if (upstream) ResultProcessor_Free(upstream);
}

/* A Query represents the parse tree and execution plan for a single search
 * query */
typedef struct RSQuery {
  // the raw query text
  char *raw;
  // the raw text len
  size_t len;
  // the token count
  int numTokens;
  int tokenId;
  uint32_t total;
  // paging offset
  size_t offset;
  // paging limit
  size_t limit;

  // field Id bitmask
  t_fieldMask fieldMask;

  // the query execution stage at the root of the query
  QueryNode *root;

  IndexIterator *rootFilter;

  ResultProcessor *rootProcessor;
  SearchResultCtx resultCtx;
  // Document metatdata table, to be used during execution
  DocTable *docTable;

  RedisSearchCtx *ctx;

  ConcurrentSearchCtx conc;

  int maxSlop;
  // Whether phrases are in order or not
  int inOrder;

  int aborted;

  int concurrentMode;

  // Query expander
  RSQueryTokenExpander expander;
  RSFreeFunction expanderFree;
  RSQueryExpanderCtx expCtx;

  // Custom scorer
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  RSScoringFunctionCtx scorerCtx;

  // sorting key by specific inline field
  RSSortingKey *sortKey;

  const char *language;

  StopWordList *stopwords;

  RSPayload payload;

  RSSearchRequest *req;
} Query;

/* Set the concurrent mode of the query. By default it's on, setting here to 0 will turn it off,
 * resulting in the query not performing context switches */
void Query_SetConcurrentMode(Query *q, int concurrent);

/* Evaluate a query stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(Query *q, QueryNode *n);

/* Build the processor chain of the query, returning the root processor */
ResultProcessor *Query_BuildProcessorChain(Query *q, RSSearchRequest *r);

/* Free the query execution stage and its children recursively */
void QueryNode_Free(QueryNode *n);
QueryNode *NewTokenNode(Query *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(Query *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewPrefixNode(Query *q, const char *s, size_t len);
QueryNode *NewNotNode(QueryNode *n);
QueryNode *NewOptionalNode(QueryNode *n);
QueryNode *NewNumericNode(NumericFilter *flt);
QueryNode *NewIdFilterNode(IdFilter *flt);
QueryNode *NewWildcardNode();
QueryNode *NewGeofilterNode(GeoFilter *flt);

void Query_SetNumericFilter(Query *q, NumericFilter *nf);
void Query_SetGeoFilter(Query *q, GeoFilter *gf);
void Query_SetIdFilter(Query *q, IdFilter *f);

/* Return a string representation of the query parse tree. The string should be freed by the caller
 */
const char *Query_DumpExplain(Query *q);

/* Only used in tests, for now */
void QueryNode_Print(Query *q, QueryNode *qs, int depth);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1

/* Initialize a new query object from user input. This does not parse the query
 * just yet */
Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int limit,
                t_fieldMask fieldMask, int verbatim, const char *lang, StopWordList *stopwords,
                const char *expander, int maxSlop, int inOrder, const char *scorer,
                RSPayload payload, RSSortingKey *sortKey);

Query *NewQueryFromRequest(RSSearchRequest *req);
void Query_Expand(Query *q);
/* Free a query object */
void Query_Free(Query *q);

/* Lazily execute the parsed query and all its stages, and return a final result
 * object */
int Query_Execute(Query *query);

QueryNode *Query_Parse(Query *q, char **err);

#endif
