#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>
#include "redismodule.h"
#include "index.h"
#include "tokenize.h"
#include "spec.h"
#include "redis_index.h"
#include "numeric_index.h"
// QueryOp marks a query stage with its respective "op" in the query processing tree
typedef enum {
    Q_INTERSECT,
    Q_UNION,
    Q_EXACT,
    Q_LOAD,
    Q_NUMERIC,
} QueryOp;


/* A query stage represents a single iterative execution stage of a query.
the processing of a query is done by chaining multiple query stages in a tree, 
and combining their inputs and outputs */
typedef struct queryStage {
    void *value;
    int valueFreeable;
    QueryOp op;
    
    struct queryStage **children;
    struct queryStage *parent;
    int nchildren;
} QueryStage;



/* A Query represents the parse tree and execution plan for a single search query */
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
    QueryStage *root;
    // Document metatdata table, to be used during execution
    DocTable *docTable;
    
    RedisSearchCtx *ctx;
} Query;


/* QueryResult represents the final processed result of a query execution */
typedef struct queryResult {
    size_t totalResults;
    RedisModuleString **ids;
    size_t numIds;
    
    
    int error;
    char *errorString;
} QueryResult;


/* Evaluate a query stage and prepare it for execution. As execution is lazy this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalStage(Query *q, QueryStage *s);

/* Free the query execution stage and its children recursively */
void QueryStage_Free(QueryStage *s);
QueryStage *NewTokenStage(const char *term);
QueryStage *NewLogicStage(QueryOp op);
QueryStage *NewNumericStage(NumericFilter *flt);


IndexIterator *query_EvalLoadStage(Query *q, QueryStage *stage);
IndexIterator *query_EvalIntersectStage(Query *q, QueryStage *stage);
IndexIterator *query_EvalUnionStage(Query *q, QueryStage *stage);
IndexIterator *query_EvalExactIntersectStage(Query *q, QueryStage *stage);
IndexIterator *query_EvalNumericStage(Query *q, QueryStage *stage);
IndexIterator *Query_EvalStage(Query *q, QueryStage *s);


#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1


void QueryStage_AddChild(QueryStage *parent, QueryStage *child);
    

/* Initialize a new query object from user input. This does not parse the query just yet */
Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int limit,
                u_char fieldMask);
/* Free a query object */ 
void Query_Free(Query *q);
/* Tokenize the raw query and build the execution plan */
int Query_Tokenize(Query *q);

/* Lazily execute the parsed query and all its stages, and return a final result object */
QueryResult *Query_Execute(Query *query); 

void QueryResult_Free(QueryResult *q);

#endif