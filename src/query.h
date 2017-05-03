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

/* A Query represents the parse tree and execution plan for a single search
 * query */
typedef struct RSQuery {
  // the raw query text
  char *raw;
  // the raw text len
  size_t len;
  // the token count
  int numTokens;

  // paging offset
  size_t offset;
  // paging limit
  size_t limit;

  // field Id bitmask
  t_fieldMask fieldMask;

  // the query execution stage at the root of the query
  QueryNode *root;
  // Document metatdata table, to be used during execution
  DocTable *docTable;

  RedisSearchCtx *ctx;

  int maxSlop;
  // Whether phrases are in order or not
  int inOrder;

  // Query expander
  RSQueryTokenExpander expander;
  RSFreeFunction expanderFree;
  RSQueryExpanderCtx expCtx;

  // Custom scorer
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  RSScoringFunctionCtx scorerCtx;

  // If the query is sortable by embedded properties, we save the name of the sortable property and
  // its index in the sorting vector
  const char *sortBy;
  int sortIndex;

  const char *language;

  const char **stopwords;

  RSPayload payload;
} Query;

typedef struct {
  const char *id;
  double score;
  RSPayload *payload;
} ResultEntry;

/* QueryResult represents the final processed result of a query execution */
typedef struct queryResult {
  size_t totalResults;
  size_t numResults;
  ResultEntry *results;
  int error;
  char *errorString;
} QueryResult;

/* Serialize a query result to the redis client. Returns REDISMODULE_OK/ERR */
int QueryResult_Serialize(QueryResult *r, RedisSearchCtx *ctx, int nocontent, int withscores,
                          int withpayloads);

/* Evaluate a query stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(Query *q, QueryNode *n);

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
void Query_SetNumericFilter(Query *q, NumericFilter *nf);
void Query_SetGeoFilter(Query *q, GeoFilter *gf);
void Query_SetIdFilter(Query *q, IdFilter *f);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1

/* Initialize a new query object from user input. This does not parse the query
 * just yet */
Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int limit,
                t_fieldMask fieldMask, int verbatim, const char *lang, const char **stopwords,
                const char *expander, int maxSlop, int inOrder, const char *scorer,
                RSPayload payload, const char *sortBy);
void Query_Expand(Query *q);
/* Free a query object */
void Query_Free(Query *q);

/* Lazily execute the parsed query and all its stages, and return a final result
 * object */
QueryResult *Query_Execute(Query *query);

void QueryResult_Free(QueryResult *q);

QueryNode *Query_Parse(Query *q, char **err);

#endif
