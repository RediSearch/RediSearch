#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>
#include "redismodule.h"
#include "index.h"
#include "query_parser/tokenizer.h"
#include "spec.h"
#include "redis_index.h"
#include "numeric_index.h"
#include "query_node.h"

/* forward declaration to avoid include loop */
struct QueryExpander;

/* A Query represents the parse tree and execution plan for a single search
 * query */
typedef struct query {
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
  u_char fieldMask;

  // the query execution stage at the root of the query
  QueryNode *root;
  // Document metatdata table, to be used during execution
  DocTable *docTable;

  RedisSearchCtx *ctx;

  struct QueryExpander *expander;

  const char *language;

  const char **stopwords;
} Query;

typedef struct {
  RedisModuleString *id;
  double score;
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
int QueryResult_Serialize(QueryResult *r, RedisSearchCtx *ctx, int nocontent,
                          int withscores);

/* Evaluate a query stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(Query *q, QueryNode *n);

/* Free the query execution stage and its children recursively */
void QueryNode_Free(QueryNode *n);
QueryNode *NewTokenNode(Query *q, const char *s, size_t len);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewNumericNode(NumericFilter *flt);

IndexIterator *query_EvalTokenNode(Query *q, QueryTokenNode *node);
IndexIterator *query_EvalPhraseNode(Query *q, QueryPhraseNode *node);
IndexIterator *query_EvalUnionNode(Query *q, QueryUnionNode *node);
IndexIterator *query_EvalNumericNode(Query *q, QueryNumericNode *node);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1

/* Initialize a new query object from user input. This does not parse the query
 * just yet */
Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset,
                int limit, u_char fieldMask, int verbatim, const char *lang,
                const char **stopwords, const char *expander);
void Query_Expand(Query *q);
/* Free a query object */
void Query_Free(Query *q);

/* Lazily execute the parsed query and all its stages, and return a final result
 * object */
QueryResult *Query_Execute(Query *query);

void QueryResult_Free(QueryResult *q);

QueryNode *Query_Parse(Query *q, char **err);

#endif
