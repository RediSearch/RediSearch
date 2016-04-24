#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>
#include "redismodule.h"
#include "index.h"
#include "tokenize.h"

typedef enum {
    Q_INTERSECT,
    Q_UNION,
    Q_EXACT,
    Q_LOAD
} QueryOp;


typedef struct queryStage {
    const char *term;
    QueryOp op;
    
    struct queryStage **children;
    int nchildren;
} QueryStage;



typedef struct {
    const char *raw;
    size_t offset;
    size_t limit;
        
    QueryStage *root;
    RedisModuleCtx *ctx;
} Query;


typedef struct {
    size_t totalResults;
    RedisModuleString **ids;
    size_t numIds;
    
    
    int error;
    char *errorString;
} QueryResult;


IndexIterator *Query_EvalStage(RedisModuleCtx *ctx, QueryStage *s);
void QueryStage_Free(QueryStage *s);
QueryStage *NewQueryStage(const char *term, QueryOp op);
IndexIterator *evalLoadStage(RedisModuleCtx *ctx, QueryStage *stage);
IndexIterator *evalIntersectStage(RedisModuleCtx *ctx, QueryStage *stage);
IndexIterator *evalUnionStage(RedisModuleCtx *ctx, QueryStage *stage);
IndexIterator *evalExactIntersectStage(RedisModuleCtx *ctx, QueryStage *stage);
IndexIterator *Query_EvalStage(RedisModuleCtx *ctx, QueryStage *s);


#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1

IndexIterator *Query_EvalStage(RedisModuleCtx *ctx, QueryStage *s);

void QueryStage_AddChild(QueryStage *parent, QueryStage *child);
    
int queryTokenFunc(void *ctx, Token t);
Query *ParseQuery(const char *query, int offset, int limit); 
void Query_Free(Query *q);
u_int32_t getHitScore(void * ctx); 
QueryResult *Query_Execute(RedisModuleCtx *ctx, Query *query); 
void QueryResult_Free(QueryResult *q);

#endif