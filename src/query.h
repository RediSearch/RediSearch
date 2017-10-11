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

/* A QueryParseCtx represents the parse tree and execution plan for a single search
 * QueryParseCtx */
typedef struct RSQuery {
  // the raw query text
  char *raw;
  // the raw text len
  size_t len;

  // the token count
  int numTokens;

  // the current token id (we assign ids to all token nodes)
  int tokenId;

  // Stopword list
  StopWordList *stopwords;

  const char *language;
  // parsing state
  int ok;

  // Index spec
  RedisSearchCtx *sctx;

  // query root
  QueryNode *root;

  char *errorMsg;

  t_fieldMask fieldMask;
} QueryParseCtx;

typedef struct {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  int numTokens;
  int tokenId;
  RSSearchRequest *req;
  DocTable *docTable;
} QueryEvalCtx;

/* Evaluate a QueryParseCtx stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n);

/* Free the QueryParseCtx execution stage and its children recursively */
void QueryNode_Free(QueryNode *n);
QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(QueryParseCtx *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewPrefixNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewNotNode(QueryNode *n);
QueryNode *NewOptionalNode(QueryNode *n);
QueryNode *NewNumericNode(NumericFilter *flt);
QueryNode *NewIdFilterNode(IdFilter *flt);
QueryNode *NewWildcardNode();
QueryNode *NewGeofilterNode(GeoFilter *flt);

void Query_SetNumericFilter(QueryParseCtx *q, NumericFilter *nf);
void Query_SetGeoFilter(QueryParseCtx *q, GeoFilter *gf);
void Query_SetIdFilter(QueryParseCtx *q, IdFilter *f);

/* Only used in tests, for now */
void QueryNode_Print(QueryParseCtx *q, QueryNode *qs, int depth);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing QueryParseCtx"
#define QUERY_ERROR_INTERNAL -1

/* Initialize a new QueryParseCtx object from user input. This does not parse the QueryParseCtx
 * just yet */
QueryParseCtx *NewQueryParseCtx(RSSearchRequest *req);
QueryNode *Query_Parse(QueryParseCtx *q, char **err);

void Query_Expand(QueryParseCtx *q, const char *expander);

/* Return a string representation of the QueryParseCtx parse tree. The string should be freed by
 * the
 * caller
 */
const char *Query_DumpExplain(QueryParseCtx *q);

/* Free a QueryParseCtx object */
void Query_Free(QueryParseCtx *q);

/******************************************************************************************************
 *   Result Processor Definitions
 ******************************************************************************************************/
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
} QueryExecutionCtx;

typedef struct {
  void *privdata;
  struct resultProcessor *upstream;
  QueryExecutionCtx *qxc;
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

/* Helper function - get the total from a processor, and if the Total callback is NULL, climb up
 * the
 * chain until we find a processor with a Total callback. This allows processors to avoid
 * implementing it if they have no calculations to add to Total (such as deeted/ignored results)
 * */
static inline size_t ResultProcessor_Total(ResultProcessor *rp) {
  return rp->ctx.qxc->totalResults;
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

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

typedef struct {
  RedisSearchCtx *ctx;

  IndexIterator *rootFilter;

  ResultProcessor *rootProcessor;

  QueryExecutionCtx execCtx;

  ConcurrentSearchCtx conc;

  RSSearchRequest *req;

} QueryPlan;

/* Set the concurrent mode of the QueryParseCtx. By default it's on, setting here to 0 will turn
 * it
 * off,
 * resulting in the QueryParseCtx not performing context switches */
void Query_SetConcurrentMode(QueryPlan *q, int concurrent);

/* Build the processor chain of the QueryParseCtx, returning the root processor */
QueryPlan *Query_BuildBlan(QueryParseCtx *parsedQuery, RSSearchRequest *req);

ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, RSSearchRequest *req);
/* Lazily execute the parsed QueryParseCtx and all its stages, and return a final result
 * object */
int QueryPlan_Execute(QueryPlan *ctx, const char **err);

#endif
